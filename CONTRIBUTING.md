# Contributing

If you use Windows, use it only to enter WSL. All development happens on
Linux inside the repository's
[VS Code devcontainer](https://code.visualstudio.com/docs/devcontainers/containers).
CI uses the same development environment.

## First-time setup

1. Install VS Code and its WSL and Dev Containers extensions from PowerShell:

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   code --install-extension ms-vscode-remote.remote-wsl
   code --install-extension ms-vscode-remote.remote-containers
   ```

1. Clone your fork on Windows, then check out and push the branch you want to
   work on:

   ```powershell
   git clone https://github.com/<your-github-user>/rust-playground.git
   ```

1. From an **elevated PowerShell 7** prompt in the checkout, install
   [Task](https://taskfile.dev) and run
   [`dev/bootstrap-dev-env.yaml`](dev/bootstrap-dev-env.yaml):

   > ⚠️ This **deletes every WSL distribution** on the machine before
   > recreating `Ubuntu-24.04`. Push any work inside WSL first.

   ```powershell
   winget install Task.Task
   $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
   task --taskfile dev/bootstrap-dev-env.yaml
   ```

   Without any prompts, this recreates WSL on the drive with the most free
   space and clones your current branch, with submodules, into
   `/workspaces/rust-playground`. Git inside WSL reuses your Git for Windows
   credentials and identity. It then runs the idempotent
   [Linux host bootstrap](dev/bootstrap-dev-env.sh), which installs Azure CLI,
   GitHub CLI, Docker, Node.js, and the Dev Containers CLI, signs in the Azure
   CLI through your signed-in browser with single sign-on, restarts Docker, and
   clears existing Docker containers, volumes, and networks. Finally, it runs
   `npm ci`, starts the devcontainer, and opens it in VS Code. After a restart,
   run `task --taskfile dev/bootstrap-dev-env.yaml up` instead; it keeps WSL
   and reopens the devcontainer.

   Both paths verify the VS Code CLI and required extensions before changing
   WSL or Docker. CLI and extension failures stop the bootstrap immediately.
   Repair or reinstall VS Code if this preflight fails; when using
   repo-bootstrapper, its prerequisite step automatically repairs a broken CLI
   before invoking this Taskfile.

   The devcontainer initialization creates the host credential directories
   used for the read/write mounts at `~/.azure` and `~/.config/gh`.
   Repository dependencies are installed inside the container by its
   post-create command.

1. Authenticate from inside the devcontainer when needed:

   ```bash
   az login
   gh auth login
   ```

   For local package publication, add the `write:packages` scope:

   ```bash
   gh auth refresh --hostname github.com --scopes write:packages
   ```

   Authentication changes are shared with the WSL host through the mounted
   credential directories. Never copy either directory into the repository.

## Verify changes

The Windows bootstrap preflight has non-destructive regression checks that
mock the VS Code CLI. Run them on the Windows host with PowerShell 7:

```powershell
pwsh -NoProfile -NonInteractive -File dev/initialize-vscode.Tests.ps1
```

Run checks inside the devcontainer:

```bash
npx nx run-many -t verify --all --parallel=1
npx nx run hello-world:image-smoke
```

For Rust-only changes:

```bash
npx nx run rust:verify
```

For collector source development and submodule updates, see the
[otelcol-rust README](projects/otelcol-rust/README.md). For the Compose demo,
local Parquet output, and E2E verification, see the
[loadtest README](projects/loadtest/README.md).

## Build the devcontainer from source

The default workflow uses the immutable image pinned in
`.devcontainer/docker-compose.yml`. Only use the source configuration when
changing the devcontainer itself:

```bash
npx nx run devcontainer:down
npx nx run devcontainer:up-source
```

In VS Code, choose **Dev Containers: Attach to Running Container** and select
the `rust-playground (source)` container.

## Stop the devcontainer

After closing VS Code, run this from the WSL host:

```bash
npx nx run devcontainer:down
```

Add `-- --volumes` only when the workspace's named volumes should also be
deleted:

```bash
npx nx run devcontainer:down -- --volumes
```

The cleanup target affects only this workspace and never performs a global
Docker prune.

For editor-free operation, monitoring, and troubleshooting, see
[Headless devcontainer operations](docs/devcontainer/headless-operations.md).
For image publication, see
[Container publishing](docs/containers/publishing.md).
