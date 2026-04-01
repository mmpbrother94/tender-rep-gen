$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $projectRoot "generated\deploy_logs"
$appPidFile = Join-Path $logDir "app.pid"
$tunnelPidFile = Join-Path $logDir "tunnel.pid"

foreach ($pidFile in @($appPidFile, $tunnelPidFile)) {
    if (-not (Test-Path $pidFile)) {
        continue
    }
    $pidValue = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($pidValue) {
        Get-Process -Id $pidValue -ErrorAction SilentlyContinue | Stop-Process -Force
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}
