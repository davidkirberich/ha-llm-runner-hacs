$ErrorActionPreference = "Stop"
$global:LlmRunnerDeployTest = @{
    Captured = @(); Uploads = 0; FailUpload = $false; LocalArchive = $null
}

function ssh {
    [CmdletBinding(PositionalBinding = $false)]
    param(
        [Parameter(ValueFromRemainingArguments = $true)]$Arguments,
        [Parameter(ValueFromPipeline = $true)][string]$InputLine
    )
    begin { $lines = @() }
    process { $lines += $InputLine }
    end {
        $global:LlmRunnerDeployTest.Captured += ($lines -join "`n")
        $global:LASTEXITCODE = 0
    }
}

function scp {
    [CmdletBinding(PositionalBinding = $false)]
    param([Parameter(ValueFromRemainingArguments = $true)]$Arguments)
    $global:LlmRunnerDeployTest.Uploads++
    $global:LlmRunnerDeployTest.LocalArchive = $Arguments[-2]
    if (-not (Test-Path -LiteralPath $global:LlmRunnerDeployTest.LocalArchive)) { throw "Upload archive is missing." }
    if ($global:LlmRunnerDeployTest.FailUpload) { $global:LASTEXITCODE = 1 } else { $global:LASTEXITCODE = 0 }
}

function Assert([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

$deployScript = Join-Path $PSScriptRoot "..\scripts\deploy-local.ps1"
$manifestPath = Join-Path $PSScriptRoot "..\custom_components\llm_runner\manifest.json"
$manifestBefore = Get-Content $manifestPath -Raw
& $deployScript -CheckOnly
Assert ($global:LlmRunnerDeployTest.Captured.Count -eq 1) "CheckOnly must only run preflight."
Assert ($global:LlmRunnerDeployTest.Uploads -eq 0) "CheckOnly must not upload."

$global:LlmRunnerDeployTest.Captured = @()
& $deployScript
Assert ($global:LlmRunnerDeployTest.Uploads -eq 1) "Deploy must upload once."
Assert ($global:LlmRunnerDeployTest.Captured.Count -eq 3) "Deploy must run preflight, deploy and cleanup."
Assert (-not (Test-Path -LiteralPath $global:LlmRunnerDeployTest.LocalArchive)) "Local archive must be cleaned."
Assert ($global:LlmRunnerDeployTest.Captured[1].Contains('trap cleanup EXIT')) "Deploy must clean temporary staging."
Assert ($global:LlmRunnerDeployTest.Captured[1].Contains('mv "$BACKUP/llm_runner" "$TARGET"')) "Deploy must restore backup on failure."
Assert ($global:LlmRunnerDeployTest.Captured[1] -match '\d+\.\d+\.\d+-preview\.[a-f0-9]+(?:\.dirty)?') "Deploy must label the uploaded version."
Assert ($global:LlmRunnerDeployTest.Captured[1].Contains('.version = $version')) "Deploy must update staged manifest."
Assert ((Get-Content $manifestPath -Raw) -ceq $manifestBefore) "Repository manifest must not change."

$deployBody = $global:LlmRunnerDeployTest.Captured[1]
$global:LlmRunnerDeployTest.Captured = @()
& $deployScript -Remove
Assert ($global:LlmRunnerDeployTest.Captured.Count -eq 2) "Removal must run preflight and removal."
Assert ($global:LlmRunnerDeployTest.Uploads -eq 1) "Removal must not upload."
Assert ($global:LlmRunnerDeployTest.Captured[1].Contains('any(.domain == "llm_runner")')) "Removal must guard configured entries."
Assert ($global:LlmRunnerDeployTest.Captured[1].Contains('mv "$TARGET" "$BACKUP/llm_runner"')) "Removal must preserve backup."
$removeBody = $global:LlmRunnerDeployTest.Captured[1]

$global:LlmRunnerDeployTest.Captured = @()
$global:LlmRunnerDeployTest.FailUpload = $true
$failed = $false
try { & $deployScript } catch { $failed = $_.Exception.Message -eq "Upload failed." }
Assert $failed "Upload failure must be surfaced."
Assert (-not (Test-Path -LiteralPath $global:LlmRunnerDeployTest.LocalArchive)) "Failed upload must clean local archive."
Assert ($global:LlmRunnerDeployTest.Captured.Count -eq 2) "Failed upload must run preflight and remote cleanup only."

foreach ($body in @($deployBody, $removeBody) + $global:LlmRunnerDeployTest.Captured) {
    Assert ($body -notmatch '(?m)^\s*ha\s+(core|host)\s+(restart|reboot)') "Script must never restart HA."
}

$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path $deployScript), [ref]$tokens, [ref]$parseErrors
) | Out-Null
Assert ($parseErrors.Count -eq 0) "Deployment script must parse without errors."
Write-Output "Deployment checks passed: preflight, deploy, removal, upload failure and no restart."

# Optional Bash syntax validation through SSH; does not execute the remote bodies.
if ($env:LLM_RUNNER_TEST_SSH) {
    foreach ($body in @($deployBody, $removeBody)) {
        $body | & (Get-Command ssh -CommandType Application).Source -o BatchMode=yes $env:LLM_RUNNER_TEST_SSH "tr -d '\r' | bash -n"
        Assert ($LASTEXITCODE -eq 0) "Remote script must pass Bash syntax validation."
    }
    Write-Output "Remote Bash syntax checks passed."
}
Remove-Variable LlmRunnerDeployTest -Scope Global
