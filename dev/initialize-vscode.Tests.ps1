$ErrorActionPreference = 'Stop'
$script = Get-Content (Join-Path $PSScriptRoot 'initialize-vscode.ps1') -Raw

foreach ($failure in @('', '--version', '--list-extensions', '--uninstall-extension', 'ms-vscode-remote.remote-wsl', 'ms-vscode-remote.remote-containers')) {
    $global:Calls = [Collections.Generic.List[string]]::new()
    function code {
        $command = $args -join ' '
        $global:Calls.Add($command)
        $global:LASTEXITCODE = if ($failure -and $command.Contains($failure)) { 17 } else { 0 }
        if ($args[0] -eq '--list-extensions') { 'Microsoft.wavework' }
        if ($args[0] -eq '--version') { '1.100.0' }
    }

    $caught = $null
    try { & ([scriptblock]::Create($script)) | Out-Null }
    catch { $caught = $_ }
    if ($failure) {
        if (-not $caught -or "$caught" -notmatch 'exit code 17') { throw "Expected checked failure for $failure, got $caught" }
        if (-not $global:Calls[-1].Contains($failure)) { throw "Commands continued after failure: $($global:Calls -join ', ')" }
    } else {
        if ($caught) { throw $caught }
        if ($global:Calls.Count -ne 5) { throw "Expected version, list, uninstall and two installs; got $($global:Calls.Count) calls" }
    }
}
Remove-Item Function:\code
$bootstrap = Get-Content (Join-Path $PSScriptRoot 'bootstrap-dev-env.ps1') -Raw
if ($bootstrap.IndexOf("'initialize-vscode.ps1'") -lt 0 -or
    $bootstrap.IndexOf("'initialize-vscode.ps1'") -gt $bootstrap.IndexOf('wsl -')) {
    throw 'Full bootstrap must check VS Code before invoking WSL.'
}
$taskfile = Get-Content (Join-Path $PSScriptRoot 'bootstrap-dev-env.yaml') -Raw
$up = $taskfile.Substring($taskfile.IndexOf('  up:'))
if ($up.IndexOf('initialize-vscode.ps1') -lt 0 -or
    $up.IndexOf('initialize-vscode.ps1') -gt $up.IndexOf('wsl -')) {
    throw 'Up must check VS Code before invoking WSL.'
}
Write-Output 'VS Code preflight regression cases passed.'
# GitHub's PowerShell wrapper propagates LASTEXITCODE, including deliberate mock failures.
$global:LASTEXITCODE = 0
