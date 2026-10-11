#!/usr/bin/env node

import { lstatSync, realpathSync, rmSync } from 'node:fs';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

const temporaryDirectories = ['.temp', '.ci-logs', 'logs', 'coverage'];
const protectedDirectories = new Set([
  '.git',
  '.nx',
  '.cargo',
  '.cache',
  'node_modules',
  'target',
  'debug',
  'submodules',
]);

function statIfPresent(filePath: string): ReturnType<typeof lstatSync> | undefined {
  try {
    return lstatSync(filePath);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

export function cleanProject(
  projectRoot: string,
  workspace: string = process.cwd(),
): string[] {
  const root = realpathSync(workspace);
  const project = resolve(root, projectRoot);
  const projectPath = relative(root, project);
  if (
    !projectRoot.trim() ||
    isAbsolute(projectRoot) ||
    projectPath === '..' ||
    projectPath.startsWith(`..${sep}`) ||
    isAbsolute(projectPath)
  ) {
    throw new Error('Project root must be a relative path inside the workspace.');
  }

  const segments = projectPath ? projectPath.split(sep) : [];
  if (segments.some((segment) => protectedDirectories.has(segment.toLowerCase()))) {
    throw new Error('Refusing to clean a managed cache, Git directory, or submodule.');
  }
  let directory = root;
  for (const segment of segments) {
    directory = join(directory, segment);
    const stat = statIfPresent(directory);
    if (!stat?.isDirectory() || stat.isSymbolicLink()) {
      throw new Error(`Project directory must exist and not be a symlink: ${directory}`);
    }
  }
  const manifest = statIfPresent(join(project, 'project.json'));
  if (!manifest?.isFile() || manifest.isSymbolicLink()) {
    throw new Error(`Expected a regular project.json in ${project}.`);
  }

  const paths = temporaryDirectories.map((name) => join(project, name));
  if (projectPath === join('projects', 'loadtest')) {
    paths.push(join(root, 'onelake'));
  }
  const removed: string[] = [];
  for (const filePath of paths) {
    if (statIfPresent(filePath)) {
      // rm removes directory symlinks themselves, never their external targets.
      rmSync(filePath, { recursive: true, force: true });
      removed.push(relative(root, filePath));
    }
  }
  return removed;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  try {
    const [projectRoot, ...extra] = process.argv.slice(2);
    if (projectRoot === undefined || extra.length > 0) {
      throw new Error('Usage: clean.ts <workspace-relative-project-root>');
    }
    const removed = cleanProject(projectRoot);
    console.log(
      removed.length
        ? `Removed temporary state: ${removed.join(', ')}`
        : `No temporary state to clean for ${projectRoot}.`,
    );
  } catch (error) {
    process.stderr.write(`clean: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
