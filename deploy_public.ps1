$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $projectRoot "generated\deploy_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$appStdOut = Join-Path $logDir "app_stdout.log"
$appStdErr = Join-Path $logDir "app_stderr.log"
$tunnelStdOut = Join-Path $logDir "tunnel_stdout.log"
$tunnelStdErr = Join-Path $logDir "tunnel_stderr.log"
$urlFile = Join-Path $logDir "public_url.txt"
$appPidFile = Join-Path $logDir "app.pid"
$tunnelPidFile = Join-Path $logDir "tunnel.pid"
$cloudflaredPath = Join-Path $projectRoot "generated\deploy_tools\cloudflared.exe"

if (Test-Path $appPidFile) {
    $existingPid = Get-Content $appPidFile -ErrorAction SilentlyContinue
    if ($existingPid) {
        Get-Process -Id $existingPid -ErrorAction SilentlyContinue | Stop-Process -Force
    }
    Remove-Item $appPidFile -ErrorAction SilentlyContinue
}

if (Test-Path $tunnelPidFile) {
    $existingPid = Get-Content $tunnelPidFile -ErrorAction SilentlyContinue
    if ($existingPid) {
        Get-Process -Id $existingPid -ErrorAction SilentlyContinue | Stop-Process -Force
    }
    Remove-Item $tunnelPidFile -ErrorAction SilentlyContinue
}

Remove-Item $appStdOut, $appStdErr, $tunnelStdOut, $tunnelStdErr, $urlFile -ErrorAction SilentlyContinue

$appProcess = Start-Process -FilePath "pythonw.exe" `
    -ArgumentList "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", "8000", "--no-server-header" `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $appStdOut `
    -RedirectStandardError $appStdErr `
    -PassThru

$appProcess.Id | Set-Content $appPidFile

$healthy = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    Start-Sleep -Seconds 1
    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -TimeoutSec 5
        if ($response.status -eq "ok") {
            $healthy = $true
            break
        }
    }
    catch {
    }
}

if (-not $healthy) {
    throw "Application failed to start. Check $appStdErr"
}

if (Test-Path $cloudflaredPath) {
    $tunnelProcess = Start-Process -FilePath $cloudflaredPath `
        -ArgumentList "tunnel", "--url", "http://127.0.0.1:8000", "--no-autoupdate" `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $tunnelStdOut `
        -RedirectStandardError $tunnelStdErr `
        -PassThru
}
else {
    $tunnelProcess = Start-Process -FilePath "ssh.exe" `
        -ArgumentList "-o", "StrictHostKeyChecking=no", "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3", "-o", "TCPKeepAlive=yes", "-o", "ExitOnForwardFailure=yes", "-R", "80:127.0.0.1:8000", "nokey@localhost.run" `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $tunnelStdOut `
        -RedirectStandardError $tunnelStdErr `
        -PassThru
}

$tunnelProcess.Id | Set-Content $tunnelPidFile

$publicUrl = $null
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    Start-Sleep -Seconds 1
    foreach ($logPath in @($tunnelStdErr, $tunnelStdOut)) {
        if (-not (Test-Path $logPath)) {
            continue
        }
        $cloudflareMatch = Select-String -Path $logPath -Pattern "https://[A-Za-z0-9.-]+\.trycloudflare\.com" -AllMatches -ErrorAction SilentlyContinue | Select-Object -Last 1
        if ($cloudflareMatch) {
            $urlMatch = [regex]::Match($cloudflareMatch.Line, "https://[A-Za-z0-9.-]+\.trycloudflare\.com")
            if ($urlMatch.Success) {
                $publicUrl = $urlMatch.Value
                break
            }
        }
        $tunnelLine = Select-String -Path $logPath -Pattern "tunneled with tls termination" -ErrorAction SilentlyContinue | Select-Object -Last 1
        if ($tunnelLine) {
            $urlMatch = [regex]::Match($tunnelLine.Line, "https://[A-Za-z0-9.-]+")
            if ($urlMatch.Success) {
                $publicUrl = $urlMatch.Value
                break
            }
        }
        $lifeMatch = Select-String -Path $logPath -Pattern "https://[A-Za-z0-9.-]+\.lhr\.life" -AllMatches -ErrorAction SilentlyContinue | Select-Object -Last 1
        if ($lifeMatch) {
            $urlMatch = [regex]::Match($lifeMatch.Line, "https://[A-Za-z0-9.-]+\.lhr\.life")
            if ($urlMatch.Success) {
                $publicUrl = $urlMatch.Value
                break
            }
        }
    }
    if ($publicUrl) {
        break
    }
}

if (-not $publicUrl) {
    throw "Tunnel did not return a public URL. Check $tunnelStdErr"
}

$publicUrl | Set-Content $urlFile
Write-Output $publicUrl
