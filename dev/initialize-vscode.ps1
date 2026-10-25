$ErrorActionPreference = 'Stop'

function Invoke-Code {
    param([string[]]$Arguments)
    code @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "VS Code command 'code $($Arguments -join ' ')' failed with exit code $LASTEXITCODE. Repair or reinstall VS Code before retrying; repo-bootstrapper repairs a broken CLI automatically."
    }
}

Invoke-Code -Arguments @('--version')
$installedExtensions = @(Invoke-Code -Arguments @('--list-extensions'))
foreach ($extension in @('Microsoft.wavework')) {
    if ($extension -in $installedExtensions) {
        Write-Host "Uninstalling blocklisted VS Code extension: $extension"
        Invoke-Code -Arguments @('--uninstall-extension', $extension)
    }
}

Invoke-Code -Arguments @('--install-extension', 'ms-vscode-remote.remote-wsl')
Invoke-Code -Arguments @('--install-extension', 'ms-vscode-remote.remote-containers')
