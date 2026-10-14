import assert from 'node:assert/strict';
import test from 'node:test';

import {
  SdkGenerationTool,
  fixtureArtifacts,
  generatedOutput,
  type CommandRunner,
} from '../src/index.js';
import { runCli } from '../src/cli.js';

class RecordingRunner implements CommandRunner {
  readonly calls: Array<{
    readonly args: readonly string[];
    readonly command: string;
  }> = [];

  run(command: string, args: readonly string[]) {
    this.calls.push({ command, args: [...args] });
    return { exitCode: 0, stderr: '', stdout: '' };
  }
}

test('generate fixture runs artifact export then one shared generator', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  const result = tool.generateFixture();

  assert.equal(result.commands.length, 2);
  assert.deepEqual(
    runner.calls.map((call) => call.command),
    ['cargo', 'cargo'],
  );
  assert.ok(runner.calls[0]?.args.includes('fixture'));
  assert.ok(runner.calls[1]?.args.includes('generate'));
  assert.ok(runner.calls[1]?.args.includes(fixtureArtifacts));
  assert.ok(runner.calls[1]?.args.includes(generatedOutput));
});

test('check CLI delegates to deterministic check workflow', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  const commands = runCli(['check'], tool);

  assert.equal(commands.length, 1);
  assert.ok(runner.calls[0]?.args.includes('check'));
});

test('CLI rejects unknown workflows', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  assert.throws(() => runCli(['unknown'], tool), /Unknown arrowcol SDK workflow/u);
});
