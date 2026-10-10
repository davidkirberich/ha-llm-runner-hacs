<#
.SYNOPSIS
    Deploys or removes the companion integration over SSH. Never restarts Home Assistant.
.DESCRIPTION
    Requires SSH access to Home Assistant's configuration folder. Existing integration
    files are moved to a timestamped backup before replacement or removal. HA config
    entries, HACS metadata and automations are not modified.
.EXAMPLE
    .\scripts\deploy-local.ps1 -CheckOnly
.EXAMPLE
    .\scripts\deploy-local.ps1
.EXAMPLE
    .\scripts\deploy-local.ps1 -Remove
#>
[CmdletBinding()]
param(
    [string]$HostName = "homeassistant.local",
    [string]$User = "root",
    [ValidateRange(1, 65535)]
    [int]$Port = 22,
    [switch]$CheckOnly,
    [switch]$Remove
)

$ErrorActionPreference = "Stop"
if ($CheckOnly -and $Remove) { throw "Use -CheckOnly or -Remove, not both." }
$remote = "$User@$HostName"
$sshOptions = @("-o", "ConnectTimeout=10", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new")
$sshArgs = @("-p", $Port) + $sshOptions
foreach ($tool in "ssh", "scp", "tar") {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "'$tool' was not found. Install the Windows OpenSSH Client and tar."
    }
}

function Invoke-Remote([string]$Script, [string]$Failure) {
    $Script | ssh @sshArgs $remote "tr -d '\r' | bash -s"
    if ($LASTEXITCODE -ne 0) { throw $Failure }
}

$common = @'
set -eu
CONFIG=
for directory in /homeassistant /config; do
  if [ -f "$directory/configuration.yaml" ]; then CONFIG=$directory; break; fi
done
[ -n "$CONFIG" ] || { echo "Home Assistant configuration folder not found." >&2; exit 1; }
[ ! -L "$CONFIG/custom_components" ] || { echo "Refusing a symlinked custom_components folder." >&2; exit 1; }
TARGET=$CONFIG/custom_components/llm_runner
[ ! -L "$TARGET" ] || { echo "Refusing a symlinked integration folder." >&2; exit 1; }
if [ -e "$TARGET" ] && [ ! -d "$TARGET" ]; then
  echo "Integration target is not a directory." >&2; exit 1
fi

'@

Write-Host "Checking SSH access to $remote ..."
Invoke-Remote ($common + @'
command -v bash >/dev/null
command -v tar >/dev/null
command -v mktemp >/dev/null
command -v jq >/dev/null
[ -w "$CONFIG" ] || { echo "Configuration folder is not writable." >&2; exit 1; }
if [ -d "$CONFIG/custom_components" ]; then
  [ -w "$CONFIG/custom_components" ] || { echo "custom_components is not writable." >&2; exit 1; }
fi
echo "Configuration folder: $CONFIG"
if [ -d "$TARGET" ]; then echo "Existing integration will be backed up."; else echo "Integration is not installed."; fi
echo "Home Assistant will NOT be restarted."
'@) "SSH or configuration check failed."
if ($CheckOnly) { return }

if ($Remove) {
    Invoke-Remote ($common + @'
if [ -f "$CONFIG/.storage/core.config_entries" ]; then
  if jq -e '.data.entries | any(.domain == "llm_runner")' "$CONFIG/.storage/core.config_entries" >/dev/null; then
    echo "Remove HA LLM Runner under Settings > Devices & services first, then rerun -Remove." >&2
    exit 1
  else
    CODE=$?
    [ "$CODE" -eq 1 ] || { echo "Could not check existing config entries." >&2; exit 1; }
  fi
fi
if [ ! -d "$TARGET" ]; then echo "Integration files are already absent."; exit 0; fi
BACKUP_ROOT=$CONFIG/llm_runner_deploy_backups
[ ! -L "$BACKUP_ROOT" ] || { echo "Refusing a symlinked backup folder." >&2; exit 1; }
mkdir -p "$BACKUP_ROOT"
BACKUP=$(mktemp -d "$BACKUP_ROOT/removed-$(date +%Y%m%d-%H%M%S)-XXXXXX")
mv "$TARGET" "$BACKUP/llm_runner"
echo "Integration files removed; backup: $BACKUP/llm_runner"
echo "Restart Home Assistant Core MANUALLY to finish removal."
echo "The add-on and user automations were not changed."
'@) "Removal failed. See the output above."
    return
}

$source = Join-Path $PSScriptRoot "..\custom_components\llm_runner"
if (-not (Test-Path (Join-Path $source "manifest.json"))) { throw "Integration manifest not found." }
$manifest = Get-Content (Join-Path $source "manifest.json") -Raw | ConvertFrom-Json
if ($manifest.domain -ne "llm_runner") { throw "Unexpected integration domain." }
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$commit = git -C $repo rev-parse --short HEAD
if ($LASTEXITCODE -ne 0) { throw "Could not determine deployment commit." }
$changes = git -C $repo status --porcelain
if ($LASTEXITCODE -ne 0) { throw "Could not determine worktree status." }
$baseVersion = ([string]$manifest.version -split '[-+]')[0]
if ($baseVersion -notmatch '^\d+\.\d+\.\d+$') { throw "Expected a SemVer release version." }
$previewVersion = "$baseVersion-preview.$($commit.Trim())"
if ($changes) { $previewVersion += ".dirty" }
Write-Host "Manual deployment version: $previewVersion"
$uploadName = "llm_runner_deploy_$([guid]::NewGuid().ToString('N')).tar.gz"
$archive = Join-Path ([IO.Path]::GetTempPath()) $uploadName
$remoteArchive = "/tmp/$uploadName"
try {
    Write-Host "Packing and uploading integration ..."
    tar --exclude=__pycache__ --exclude="*.pyc" -czf $archive -C $source .
    if ($LASTEXITCODE -ne 0) { throw "Packing failed." }
    scp -P $Port @sshOptions -q $archive "${remote}:$remoteArchive"
    if ($LASTEXITCODE -ne 0) { throw "Upload failed." }

    $deploy = @'
ARCHIVE=__ARCHIVE__
STAGE=
cleanup() {
  rm -f "$ARCHIVE"
  if [ -n "$STAGE" ] && [ -d "$STAGE" ]; then rm -rf "$STAGE"; fi
}
trap cleanup EXIT
mkdir -p "$CONFIG/custom_components"
STAGE=$(mktemp -d "$CONFIG/custom_components/.llm_runner-deploy-XXXXXX")
tar -xzf "$ARCHIVE" -C "$STAGE"
[ -f "$STAGE/__init__.py" ] && [ -f "$STAGE/manifest.json" ] || {
  echo "Incomplete integration archive." >&2; exit 1;
}
jq -e '.domain == "llm_runner"' "$STAGE/manifest.json" >/dev/null
jq --arg version "__VERSION__" '.version = $version' "$STAGE/manifest.json" > "$STAGE/manifest.json.new"
mv "$STAGE/manifest.json.new" "$STAGE/manifest.json"
BACKUP=
if [ -d "$TARGET" ]; then
  BACKUP_ROOT=$CONFIG/llm_runner_deploy_backups
  [ ! -L "$BACKUP_ROOT" ] || { echo "Refusing a symlinked backup folder." >&2; exit 1; }
  mkdir -p "$BACKUP_ROOT"
  BACKUP=$(mktemp -d "$BACKUP_ROOT/deployed-$(date +%Y%m%d-%H%M%S)-XXXXXX")
  mv "$TARGET" "$BACKUP/llm_runner"
  echo "Previous files backed up to $BACKUP/llm_runner"
fi
if ! mv "$STAGE" "$TARGET"; then
  if [ -n "$BACKUP" ]; then mv "$BACKUP/llm_runner" "$TARGET"; fi
  echo "Deployment failed; previous files restored where available." >&2; exit 1
fi
STAGE=
echo "Integration deployed to $TARGET"
echo "Installed version: __VERSION__"
echo "Restart Home Assistant Core MANUALLY to load the new code."
echo "First installation: then add HA LLM Runner under Settings > Devices & services."
echo "No config entries, automations or add-on files were modified."
'@
    Invoke-Remote ($common + $deploy.Replace("__ARCHIVE__", $remoteArchive).Replace("__VERSION__", $previewVersion)) "Deployment failed. See the output above."
}
finally {
    if (Test-Path $archive) { Remove-Item -LiteralPath $archive }
    # Also clean a partial upload if scp or the remote preflight failed.
    Invoke-Remote "rm -f '$remoteArchive'" "Could not clean the temporary remote archive."
}
