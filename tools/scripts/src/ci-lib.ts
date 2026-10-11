import {
  appendFileSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';

import {
  requireImageReference,
  verificationArguments,
} from './ci-plan.js';
import {
  CommandError,
  type CommandRunner,
  runChecked,
} from './process.js';

export const CI_LABEL_NAME = 'rust-playground.ci-run';

const canonicalContainerIdPattern = /^[a-f0-9]{64}$/u;
const composeProjectFormat =
  '{{ index .Config.Labels "com.docker.compose.project" }}';
const composeProjectPattern = /^[a-z0-9][a-z0-9_-]*$/u;
const containerIdFormat = '{{ .Id }}';
const containerIdPattern = /^[a-f0-9]{12,64}$/u;
const containerWorkspace = '/workspaces/rust-playground';
const ciLabelFormat = `{{ index .Config.Labels "${CI_LABEL_NAME}" }}`;
const ciLabelValuePattern = /^\d+-\d+-(?:source|published|artifact)$/u;
const artifactImagePattern = /^rust-playground-ci:(\d+)-\d+$/u;
const artifactKeyPattern = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/u;
const imageIdPattern = /^sha256:[a-f0-9]{64}$/u;

export type DevcontainerKind = 'artifact' | 'published' | 'source';

export interface CiFileSystem {
  appendFile(filePath: string, contents: string): void;
  ensurePrivateDirectory(directoryPath: string): void;
  readDirectory(directoryPath: string): readonly string[];
  readFile(filePath: string): string;
  removeFile(filePath: string): void;
  writePrivateFile(filePath: string, contents: string): void;
}

export const systemFileSystem: CiFileSystem = {
  appendFile(filePath, contents) {
    appendFileSync(filePath, contents, 'utf8');
  },
  ensurePrivateDirectory(directoryPath) {
    mkdirSync(directoryPath, { mode: 0o700, recursive: true });
  },
  readDirectory(directoryPath) {
    return readdirSync(directoryPath);
  },
  readFile(filePath) {
    return readFileSync(filePath, 'utf8');
  },
  removeFile(filePath) {
    rmSync(filePath, { force: true });
  },
  writePrivateFile(filePath, contents) {
    writeFileSync(filePath, contents, { encoding: 'utf8', mode: 0o600 });
  },
};

export interface PrepareOptions {
  readonly fileSystem: CiFileSystem;
  readonly home: string;
  readonly workspace: string;
}

export function prepareCiEnvironment(options: PrepareOptions): void {
  const azureDirectory = path.join(options.home, '.azure');
  const githubDirectory = path.join(options.home, '.config', 'gh');

  options.fileSystem.ensurePrivateDirectory(azureDirectory);
  options.fileSystem.ensurePrivateDirectory(githubDirectory);
  options.fileSystem.writePrivateFile(
    ciEnvironmentFile(options.workspace),
    [
      `HOST_AZURE_DIR=${composeEnvironmentValue(azureDirectory)}`,
      `HOST_GH_CONFIG_DIR=${composeEnvironmentValue(githubDirectory)}`,
      '',
    ].join('\n'),
  );
}

export interface StartOptions {
  readonly githubOutput: string;
  readonly kind: DevcontainerKind;
  readonly runAttempt: string;
  readonly runId: string;
  readonly workspace: string;
  readonly image?: string;
}

export function startDevcontainer(
  options: StartOptions,
  runner: CommandRunner,
  fileSystem: CiFileSystem,
  writeOutput: (output: string) => void = (output) =>
    process.stdout.write(output),
): void {
  const ciLabel = createCiLabel(
    options.runId,
    options.runAttempt,
    options.kind,
  );
  appendStepOutputs(fileSystem, options.githubOutput, {
    ci_label: ciLabel,
    start_kind: options.kind,
  });

  if (options.kind === 'artifact') {
    prepareArtifactRuntime(options, runner, fileSystem);
  }
  const upArgs = createDevcontainerUpArgs(options, ciLabel);
  const upResult = runner.run('npx', upArgs, {
    cwd: options.workspace,
    stdout: 'capture',
  });
  if (upResult.stdout) {
    writeOutput(formatCapturedOutput(upResult.stdout));
  }

  let recoveryError: unknown;
  try {
    const outputContainerId = extractContainerId(upResult.stdout);
    const discoveredContainerIds =
      outputContainerId === undefined
        ? discoverContainerIds(ciLabel, runner)
        : [];

    if (discoveredContainerIds.length > 1) {
      throw new Error(
        `CI label ${ciLabel} matched multiple containers; refusing to choose one.`,
      );
    }

    const candidate = outputContainerId ?? discoveredContainerIds[0];
    if (candidate !== undefined) {
      const inspected = inspectStartedContainer(candidate, ciLabel, runner);
      appendStepOutputs(fileSystem, options.githubOutput, {
        container_id: inspected.containerId,
        compose_project: inspected.composeProject,
      });
    } else if (upResult.exitCode === 0) {
      throw new Error(
        'devcontainer up succeeded without returning a container ID.',
      );
    }
  } catch (error) {
    recoveryError = error;
  }

  if (upResult.exitCode !== 0) {
    throw new CommandError('npx', upArgs, upResult.exitCode);
  }
  if (recoveryError !== undefined) {
    throw recoveryError;
  }
}

export interface VerifyOptions {
  readonly containerId: string;
  readonly task?: string;
  readonly parallel?: number;
  readonly integration?: boolean;
}

export function verifyDevcontainer(
  options: VerifyOptions,
  runner: CommandRunner,
): void {
  const containerId = requireCanonicalContainerId(options.containerId);
  const args = verificationArguments(options.task, options.parallel);
  if (options.integration) {
    if (options.task !== 'loadtest:e2e') {
      throw new Error('Only loadtest:e2e may use prebuilt integration artifacts.');
    }
    args.push('--excludeTaskDependencies');
  }
  runChecked(runner, 'docker', [
    'exec',
    '--user',
    'root',
    '--workdir',
    containerWorkspace,
    containerId,
    'bash',
    '-lc',
    'install -d -m 0755 -o "$(id -u vscode)" -g "$(id -g vscode)" onelake',
  ]);
  runChecked(runner, 'docker', [
    'exec',
    '--user',
    'vscode',
    '--workdir',
    containerWorkspace,
    containerId,
    'bash',
    '-lc',
    'npx --no-install nx "$@" && npx --no-install nx run tools-scripts:clean-worktree --outputStyle=static',
    'ci-verify',
    ...args,
  ]);
}

export function verifyHost(
  options: { readonly task: string; readonly parallel?: number; readonly workspace: string },
  runner: CommandRunner,
): void {
  const args = verificationArguments(options.task, options.parallel);
  runChecked(runner, 'npx', ['--no-install', 'nx', ...args], { cwd: options.workspace });
  runChecked(runner, 'npx', [
    '--no-install', 'nx', 'run', 'tools-scripts:clean-worktree', '--outputStyle=static',
  ], { cwd: options.workspace });
}

export function buildCiEnvironment(
  options: {
    readonly baseImage: string;
    readonly githubOutput: string;
    readonly runAttempt: string;
    readonly runId: string;
    readonly workspace: string;
  },
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  const baseImage = requireImageReference(options.baseImage);
  const image = `rust-playground-ci:${requireRunNumber('GITHUB_RUN_ID', options.runId)}-${requireRunNumber('GITHUB_RUN_ATTEMPT', options.runAttempt)}`;
  const directory = path.join(options.workspace, '.temp', 'ci', 'environment');
  fileSystem.ensurePrivateDirectory(directory);
  runChecked(runner, 'npx', [
    '--no-install', 'nx', 'run', 'devcontainer:build', '--excludeTaskDependencies',
  ], { cwd: options.workspace });
  runChecked(runner, 'docker', [
    'build', '--builder', 'default', '--file', '.github/ci/Dockerfile',
    '--build-arg', `SOURCE_DEVCONTAINER=${baseImage}`, '--tag', image, '.',
  ], { cwd: options.workspace });
  runChecked(runner, 'docker', [
    'save', '--output', path.join(directory, 'environment.tar'), image,
  ]);
  runChecked(runner, 'gzip', ['--fast', '--force', path.join(directory, 'environment.tar')]);
  appendStepOutputs(fileSystem, options.githubOutput, { image });
}

export function exportImages(
  options: {
    readonly images: readonly string[];
    readonly key: string;
    readonly workspace: string;
  },
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  const key = requireArtifactKey(options.key);
  if (options.images.length === 0) throw new Error('No runtime images to export.');
  const images = options.images.map((reference) => ({
    reference: requireImageReference(reference),
    id: requireImageId(runChecked(runner, 'docker', [
      'image', 'inspect', '--format', '{{ .Id }}', reference,
    ], { stdout: 'capture' }).stdout),
  }));
  const directory = path.join(options.workspace, '.temp', 'ci', 'images');
  fileSystem.ensurePrivateDirectory(directory);
  runChecked(runner, 'docker', [
    'save', '--output', path.join(directory, `${key}.tar`), ...options.images,
  ]);
  fileSystem.writePrivateFile(path.join(directory, `${key}.json`), JSON.stringify({ images }));
}

export function importIntegrationArtifacts(
  options: { readonly images: readonly string[]; readonly workspace: string },
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  if (options.images.length === 0) throw new Error('Expected runtime images must be specified.');
  const expected = new Set(options.images.map(requireImageReference));
  const directory = path.join(options.workspace, '.temp', 'ci', 'images');
  const files = fileSystem.readDirectory(directory);
  const manifests = files.filter((file) => file.endsWith('.json')).sort();
  if (manifests.length === 0 || files.length !== manifests.length * 2) {
    throw new Error('Runtime image artifacts are missing or incomplete.');
  }
  const imported = new Set<string>();
  for (const manifest of manifests) {
    const key = requireArtifactKey(manifest.slice(0, -'.json'.length));
    if (!files.includes(`${key}.tar`)) throw new Error(`Missing image archive: ${key}`);
    const value: unknown = JSON.parse(fileSystem.readFile(path.join(directory, manifest)));
    if (!isRecord(value) || !Array.isArray(value.images) || value.images.length === 0) {
      throw new Error(`Invalid runtime image manifest: ${manifest}`);
    }
    const images = value.images.map((image: unknown) => {
      if (!isRecord(image) || typeof image.reference !== 'string' || typeof image.id !== 'string') {
        throw new Error(`Invalid runtime image entry: ${manifest}`);
      }
      const reference = requireImageReference(image.reference);
      const id = requireImageId(image.id);
      if (!expected.has(reference) || imported.has(reference)) {
        throw new Error(`Unexpected or duplicate runtime image: ${reference}`);
      }
      imported.add(reference);
      return { reference, id };
    });
    runChecked(runner, 'docker', ['load', '--input', path.join(directory, `${key}.tar`)]);
    for (const image of images) {
      const actual = requireImageId(runChecked(runner, 'docker', [
        'image', 'inspect', '--format', '{{ .Id }}', image.reference,
      ], { stdout: 'capture' }).stdout);
      if (actual !== image.id) throw new Error(`Runtime image identity mismatch: ${image.reference}`);
    }
  }
  if (imported.size !== expected.size) throw new Error('A required runtime image artifact is missing.');
  const target = path.join(options.workspace, 'target', 'ci', 'build', 'debug');
  fileSystem.ensurePrivateDirectory(target);
  runChecked(runner, 'tar', [
    '--extract', '--file', path.join(options.workspace, '.temp', 'ci', 'reader.tar'),
    '--directory', target, 'delta-inspect',
  ]);
  runChecked(runner, 'test', ['-x', path.join(target, 'delta-inspect')]);
}

export function exportReader(
  options: { readonly containerId: string; readonly workspace: string },
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  const containerId = requireCanonicalContainerId(options.containerId);
  fileSystem.ensurePrivateDirectory(path.join(options.workspace, '.temp', 'ci'));
  runChecked(runner, 'docker', [
    'exec', '--user', 'vscode', '--workdir', containerWorkspace, containerId,
    'tar', '--create', '--file', '.temp/ci/reader.tar',
    '--directory', 'target/debug', 'delta-inspect',
  ]);
}

export interface CleanupOptions {
  readonly ciLabels?: readonly string[];
  readonly composeProject?: string;
  readonly containerId?: string;
  readonly workspace: string;
}

export function cleanupDevcontainer(
  options: CleanupOptions,
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  try {
    const recordedProject = normalizeComposeProject(
      options.composeProject ?? '',
    );
    if (recordedProject !== undefined) {
      removeComposeProjects(
        new Set([recordedProject]),
        options.workspace,
        runner,
      );
      return;
    }

    const expectedLabels = normalizeCiLabels(options.ciLabels ?? []);
    const candidateIds = new Set<string>();
    const recordedContainerId = normalizeOptional(options.containerId);
    if (recordedContainerId !== undefined) {
      candidateIds.add(requireCanonicalContainerId(recordedContainerId));
    }
    for (const label of expectedLabels) {
      for (const containerId of discoverContainerIds(label, runner)) {
        candidateIds.add(containerId);
      }
    }

    const projects = new Set<string>();
    const standaloneContainers = new Set<string>();
    for (const candidateId of candidateIds) {
      const containerId = tryInspectCanonicalContainerId(candidateId, runner);
      if (containerId === undefined) {
        continue;
      }

      if (expectedLabels.length > 0) {
        const actualLabel = inspectCiLabel(containerId, runner);
        if (!expectedLabels.includes(actualLabel)) {
          throw new Error(
            `Container ${containerId} does not have an expected CI run label.`,
          );
        }
      }

      const project = inspectComposeProject(containerId, runner);
      if (project === undefined) {
        standaloneContainers.add(containerId);
      } else {
        projects.add(project);
      }
    }

    removeComposeProjects(projects, options.workspace, runner);
    for (const containerId of standaloneContainers) {
      runChecked(runner, 'docker', ['rm', '--force', containerId]);
    }
  } finally {
    fileSystem.removeFile(ciEnvironmentFile(options.workspace));
    for (const filename of ['devcontainer.json', 'compose.json']) {
      fileSystem.removeFile(path.join(options.workspace, '.temp', 'ci', filename));
    }
  }
}

export function createCiLabel(
  runId: string,
  runAttempt: string,
  kind: DevcontainerKind = 'source',
): string {
  const normalizedRunId = requireRunNumber('GITHUB_RUN_ID', runId);
  const normalizedRunAttempt = requireRunNumber(
    'GITHUB_RUN_ATTEMPT',
    runAttempt,
  );
  return `${CI_LABEL_NAME}=${normalizedRunId}-${normalizedRunAttempt}-${kind}`;
}

export function extractContainerId(output: string): string | undefined {
  try {
    const parsed = JSON.parse(output) as unknown;
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'containerId' in parsed &&
      typeof parsed.containerId === 'string'
    ) {
      return normalizeContainerReference(parsed.containerId);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

export function normalizeComposeProject(
  value: string,
): string | undefined {
  const normalized = normalizeOptional(value);
  if (normalized === undefined || normalized === '<no value>') {
    return undefined;
  }
  if (!composeProjectPattern.test(normalized)) {
    throw new Error(`Invalid Docker Compose project name: ${normalized}`);
  }
  return normalized;
}

function createDevcontainerUpArgs(
  options: StartOptions,
  ciLabel: string,
): string[] {
  const configFile = options.kind === 'artifact'
    ? path.join(options.workspace, '.temp', 'ci', 'devcontainer.json')
    : path.join(options.workspace, '.devcontainer',
      options.kind === 'source' ? path.join('source', 'devcontainer.json') : 'devcontainer.json');
  const args = [
    '--no-install',
    'devcontainer',
    'up',
    '--workspace-folder',
    options.workspace,
    '--config',
    configFile,
  ];
  if (options.kind === 'source') {
    args.push('--frozen-lockfile');
  }
  args.push('--id-label', ciLabel);
  return args;
}

function prepareArtifactRuntime(
  options: StartOptions,
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  if (options.image === undefined ||
      artifactImagePattern.exec(options.image)?.[1] !== requireRunNumber('GITHUB_RUN_ID', options.runId)) {
    throw new Error('CI artifact image must be a source-built tag from this workflow run.');
  }
  // An absent artifact must fail, never fall back to a registry image.
  runChecked(runner, 'docker', ['image', 'inspect', options.image], { stdout: 'ignore' });
  const base: unknown = JSON.parse(fileSystem.readFile(
    path.join(options.workspace, '.devcontainer', 'devcontainer.json'),
  ));
  if (!isRecord(base)) throw new Error('Invalid base devcontainer configuration.');
  const directory = path.join(options.workspace, '.temp', 'ci');
  fileSystem.ensurePrivateDirectory(directory);
  const cache = path.join(options.workspace, 'target', 'ci');
  for (const name of ['build', 'cargo-registry', 'cargo-git']) {
    fileSystem.ensurePrivateDirectory(path.join(cache, name));
  }
  const compose = path.join(directory, 'compose.json');
  fileSystem.writePrivateFile(compose, JSON.stringify({
    services: {
      workspace: {
        image: options.image,
        pull_policy: 'never',
        volumes: [
          { type: 'bind', source: path.join(cache, 'build'), target: `${containerWorkspace}/target` },
          { type: 'bind', source: path.join(cache, 'cargo-registry'), target: '/usr/local/cargo/registry' },
          { type: 'bind', source: path.join(cache, 'cargo-git'), target: '/usr/local/cargo/git' },
        ],
      },
    },
  }));
  fileSystem.writePrivateFile(path.join(directory, 'devcontainer.json'), JSON.stringify({
    ...base,
    dockerComposeFile: [path.join(options.workspace, '.devcontainer', 'docker-compose.yml'), compose],
  }));
}

function requireArtifactKey(key: string): string {
  if (!artifactKeyPattern.test(key) || key.includes('..')) {
    throw new Error(`Invalid CI artifact key: ${key}`);
  }
  return key;
}

function requireImageId(id: string): string {
  const normalized = id.trim();
  if (!imageIdPattern.test(normalized)) throw new Error(`Invalid Docker image ID: ${id}`);
  return normalized;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function inspectStartedContainer(
  candidateId: string,
  ciLabel: string,
  runner: CommandRunner,
): { readonly composeProject: string; readonly containerId: string } {
  const containerId = inspectCanonicalContainerId(candidateId, runner);
  const actualLabel = inspectCiLabel(containerId, runner);
  if (actualLabel !== ciLabel) {
    throw new Error(
      `Container ${containerId} has CI label ${actualLabel || '<no value>'}; expected ${ciLabel}.`,
    );
  }

  const composeProject = inspectComposeProject(containerId, runner);
  if (composeProject === undefined) {
    throw new Error(
      `Devcontainer ${containerId} has no valid Compose project label.`,
    );
  }
  return { composeProject, containerId };
}

function discoverContainerIds(
  ciLabel: string,
  runner: CommandRunner,
): string[] {
  normalizeCiLabel(ciLabel);
  const output = runChecked(
    runner,
    'docker',
    ['ps', '-aq', '--no-trunc', '--filter', `label=${ciLabel}`],
    { stderr: 'capture', stdout: 'capture' },
  ).stdout;
  return [
    ...new Set(
      nonEmptyLines(output).map((containerId) =>
        requireCanonicalContainerId(containerId),
      ),
    ),
  ];
}

function inspectCanonicalContainerId(
  containerId: string,
  runner: CommandRunner,
): string {
  const candidate = normalizeContainerReference(containerId);
  if (candidate === undefined) {
    throw new Error(`Invalid Docker container ID: ${containerId}`);
  }
  return requireCanonicalContainerId(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', containerIdFormat, candidate],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
}

function tryInspectCanonicalContainerId(
  containerId: string,
  runner: CommandRunner,
): string | undefined {
  const result = runner.run(
    'docker',
    ['inspect', '--format', containerIdFormat, containerId],
    { stderr: 'ignore', stdout: 'capture' },
  );
  if (result.exitCode !== 0) {
    return undefined;
  }
  return requireCanonicalContainerId(result.stdout);
}

function inspectCiLabel(
  containerId: string,
  runner: CommandRunner,
): string {
  const value = normalizeOptional(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', ciLabelFormat, containerId],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
  return value === undefined || value === '<no value>'
    ? ''
    : normalizeCiLabel(`${CI_LABEL_NAME}=${value}`);
}

function inspectComposeProject(
  containerId: string,
  runner: CommandRunner,
): string | undefined {
  return normalizeComposeProject(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', composeProjectFormat, containerId],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
}

function removeComposeProjects(
  projects: ReadonlySet<string>,
  workspace: string,
  runner: CommandRunner,
): void {
  const devcontainerDirectory = path.join(workspace, '.devcontainer');
  const composeFile = path.join(devcontainerDirectory, 'docker-compose.yml');
  for (const project of projects) {
    runChecked(runner, 'docker', [
      'compose',
      '--project-name',
      project,
      '--project-directory',
      devcontainerDirectory,
      '--file',
      composeFile,
      'down',
      '--volumes',
      '--remove-orphans',
    ]);
  }
}

function normalizeCiLabels(labels: readonly string[]): string[] {
  return [...new Set(labels.map((label) => normalizeCiLabel(label)))];
}

function normalizeCiLabel(label: string): string {
  const normalized = label.trim();
  const prefix = `${CI_LABEL_NAME}=`;
  const value = normalized.startsWith(prefix)
    ? normalized.slice(prefix.length)
    : '';
  if (!ciLabelValuePattern.test(value)) {
    throw new Error(`Invalid rust-playground CI run label: ${label}`);
  }
  return `${prefix}${value}`;
}

function requireCanonicalContainerId(value: string): string {
  const normalized = value.trim();
  if (!canonicalContainerIdPattern.test(normalized)) {
    throw new Error(`Invalid canonical Docker container ID: ${value}`);
  }
  return normalized;
}

function normalizeContainerReference(
  value: string,
): string | undefined {
  const normalized = normalizeOptional(value);
  return normalized !== undefined && containerIdPattern.test(normalized)
    ? normalized
    : undefined;
}

function requireRunNumber(name: string, value: string): string {
  const normalized = value.trim();
  if (!/^\d+$/u.test(normalized)) {
    throw new Error(`${name} must contain only decimal digits.`);
  }
  return normalized;
}

function appendStepOutputs(
  fileSystem: CiFileSystem,
  githubOutput: string,
  outputs: Readonly<Record<string, string>>,
): void {
  const contents = Object.entries(outputs)
    .map(([name, value]) => {
      if (/[\r\n]/u.test(value)) {
        throw new Error(`GitHub step output ${name} contains a newline.`);
      }
      return `${name}=${value}\n`;
    })
    .join('');
  fileSystem.appendFile(githubOutput, contents);
}

function ciEnvironmentFile(workspace: string): string {
  return path.join(workspace, '.devcontainer', '.env');
}

function composeEnvironmentValue(value: string): string {
  if (/[\0\r\n]/u.test(value)) {
    throw new Error('Host credential directory contains an unsupported character.');
  }

  const normalized = value.replaceAll('\\', '/');
  if (/^[A-Za-z0-9_./:-]+$/u.test(normalized)) {
    return normalized;
  }
  return `"${normalized
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('$', '$$')}"`;
}

function nonEmptyLines(value: string): string[] {
  return value
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
}

function normalizeOptional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}

function formatCapturedOutput(output: string): string {
  return `${output.replace(/[\r\n]+$/u, '')}\n`;
}
