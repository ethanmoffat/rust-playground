#!/usr/bin/env node

import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { Command } from 'commander';

import {
  cleanupDevcontainer,
  buildCiEnvironment,
  createCiLabel,
  exportImages,
  exportReader,
  importIntegrationArtifacts,
  type DevcontainerKind,
  prepareCiEnvironment,
  startDevcontainer,
  systemFileSystem,
  type CiFileSystem,
  verifyDevcontainer,
  verifyHost,
} from './ci-lib.js';
import {
  CommandError,
  type CommandRunner,
  SpawnCommandRunner,
} from './process.js';
import { requireImageReference } from './ci-plan.js';

const repositoryRoot = fileURLToPath(new URL('../../..', import.meta.url));

export interface CiCliDependencies {
  readonly env: Readonly<NodeJS.ProcessEnv>;
  readonly fileSystem: CiFileSystem;
  readonly repositoryRoot: string;
  readonly runner: CommandRunner;
  readonly stderr: (message: string) => void;
  readonly stdout: (message: string) => void;
}

export function createCiProgram(dependencies: CiCliDependencies): Command {
  const program = new Command()
    .name('ci')
    .description('Run the rust-playground CI devcontainer lifecycle.')
    .showHelpAfterError();

  program
    .command('prepare')
    .description('Create only the host credential state required by Compose.')
    .action(() => {
      prepareCiEnvironment({
        fileSystem: dependencies.fileSystem,
        home: requiredEnvironment('HOME', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      });
    });

  addStartCommand(program, 'source', dependencies);
  addStartCommand(program, 'published', dependencies);
  addStartCommand(program, 'artifact', dependencies);

  program
    .command('build-environment')
    .description('Build the locked source environment once and export it for CI shards.')
    .action(() => {
      buildCiEnvironment({
        baseImage: requiredEnvironment('CI_BASE_IMAGE', dependencies.env),
        githubOutput: requiredEnvironment('GITHUB_OUTPUT', dependencies.env),
        runAttempt: requiredEnvironment('GITHUB_RUN_ATTEMPT', dependencies.env),
        runId: requiredEnvironment('GITHUB_RUN_ID', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      }, dependencies.runner, dependencies.fileSystem);
    });

  program
    .command('verify-host')
    .description('Verify one dependency-closed Nx shard on the host.')
    .action(() => {
      verifyHost({
        task: requiredEnvironment('CI_TASK', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      }, dependencies.runner);
    });

  program
    .command('verify')
    .description('Run one Nx shard, or every verification target, in the exact CI container.')
    .action(() => {
      verifyDevcontainer(
        {
          containerId: requiredEnvironment(
            'CONTAINER_ID',
            dependencies.env,
          ),
          task: normalizeOptional(dependencies.env.CI_TASK),
          integration: integrationEnabled(dependencies.env.CI_INTEGRATION),
        },
        dependencies.runner,
      );
    });

  program
    .command('export-images')
    .description('Export source-built runtime images and their exact identities.')
    .action(() => {
      exportImages({
        images: imageReferences(dependencies.env.CI_IMAGE_REFS),
        key: requiredEnvironment('CI_ARTIFACT_KEY', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      }, dependencies.runner, dependencies.fileSystem);
    });

  program
    .command('export-reader')
    .description('Export the independently built Delta reader, retaining executable permissions.')
    .action(() => {
      exportReader({
        containerId: requiredEnvironment('CONTAINER_ID', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      }, dependencies.runner, dependencies.fileSystem);
    });

  program
    .command('import-integration')
    .description('Load and validate the exact images and reader required by E2E.')
    .action(() => {
      importIntegrationArtifacts({
        images: imageReferences(dependencies.env.CI_IMAGE_REFS),
        workspace: workflowWorkspace(dependencies),
      }, dependencies.runner, dependencies.fileSystem);
    });

  program
    .command('cleanup')
    .description('Remove only the recorded or labeled CI Compose resources.')
    .action(() => {
      cleanupDevcontainer(
        {
          ciLabels: cleanupLabels(dependencies.env),
          composeProject: dependencies.env.COMPOSE_PROJECT,
          containerId: dependencies.env.CONTAINER_ID,
          workspace: workflowWorkspace(dependencies),
        },
        dependencies.runner,
        dependencies.fileSystem,
      );
    });

  return program;
}

export async function runCiCli(
  argv: readonly string[],
  dependencies: CiCliDependencies,
): Promise<void> {
  try {
    await createCiProgram(dependencies).parseAsync([...argv]);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    dependencies.stderr(`ci: ${message}\n`);
    process.exitCode = error instanceof CommandError ? error.exitCode : 1;
  }
}

function addStartCommand(
  program: Command,
  kind: DevcontainerKind,
  dependencies: CiCliDependencies,
): void {
  program
    .command(`start-${kind}`)
    .description(
      kind === 'source'
        ? 'Build and start the source devcontainer Compose override.'
        : kind === 'artifact'
          ? 'Start the source-built CI artifact without rebuilding or pulling an image.'
          : 'Start the immutable published devcontainer image.',
    )
    .action(() => {
      startDevcontainer(
        {
          githubOutput: requiredEnvironment(
            'GITHUB_OUTPUT',
            dependencies.env,
          ),
          kind,
          runAttempt: requiredEnvironment(
            'GITHUB_RUN_ATTEMPT',
            dependencies.env,
          ),
          runId: requiredEnvironment('GITHUB_RUN_ID', dependencies.env),
          workspace: workflowWorkspace(dependencies),
          image: dependencies.env.CI_ARTIFACT_IMAGE,
        },
        dependencies.runner,
        dependencies.fileSystem,
        dependencies.stdout,
      );
    });
}

function cleanupLabels(
  env: Readonly<NodeJS.ProcessEnv>,
): readonly string[] {
  const explicitLabel = normalizeOptional(env.CI_LABEL);
  if (explicitLabel !== undefined) {
    return [explicitLabel];
  }

  const runId = normalizeOptional(env.GITHUB_RUN_ID);
  const runAttempt = normalizeOptional(env.GITHUB_RUN_ATTEMPT);
  if (runId === undefined && runAttempt === undefined) {
    return [];
  }
  if (runId === undefined || runAttempt === undefined) {
    throw new Error(
      'GITHUB_RUN_ID and GITHUB_RUN_ATTEMPT must both be set for label-based cleanup.',
    );
  }
  return [
    createCiLabel(runId, runAttempt, 'source'),
    createCiLabel(runId, runAttempt, 'published'),
    createCiLabel(runId, runAttempt, 'artifact'),
  ];
}

function imageReferences(value: string | undefined): readonly string[] {
  if (value === undefined) throw new Error('CI_IMAGE_REFS must be set.');
  const parsed: unknown = JSON.parse(value);
  if (!Array.isArray(parsed) || parsed.length === 0 ||
      !parsed.every((image: unknown): image is string => typeof image === 'string')) {
    throw new Error('CI_IMAGE_REFS must be a nonempty JSON array of image references.');
  }
  return parsed.map(requireImageReference);
}

function integrationEnabled(value: string | undefined): boolean {
  if (value === undefined || value === 'false') return false;
  if (value === 'true') return true;
  throw new Error('CI_INTEGRATION must be true or false.');
}

function workflowWorkspace(dependencies: CiCliDependencies): string {
  return (
    normalizeOptional(dependencies.env.GITHUB_WORKSPACE) ??
    dependencies.repositoryRoot
  );
}

function requiredEnvironment(
  name: string,
  env: Readonly<NodeJS.ProcessEnv>,
): string {
  const value = normalizeOptional(env[name]);
  if (value === undefined) {
    throw new Error(`${name} must be set.`);
  }
  return value;
}

function normalizeOptional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  await runCiCli(process.argv, {
    env: process.env,
    fileSystem: systemFileSystem,
    repositoryRoot,
    runner: new SpawnCommandRunner(),
    stderr: (message) => process.stderr.write(message),
    stdout: (message) => process.stdout.write(message),
  });
}
