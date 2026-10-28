# FV Replay watchdog: keeps `fvdebtor_creditcheck serve` running on 127.0.0.1:8787.
# Run every 5 minutes by the "FV Replay Watchdog" scheduled task (see deploy\windows_watchdog_task.ps1).
# It never starts, stops or edits cloudflared: the OI watchdog owns the tunnel.
$ErrorActionPreference = "Continue"
$root      = Split-Path -Parent $PSScriptRoot
$python    = Join-Path $root ".venv\Scripts\python.exe"
$logDir    = Join-Path $root "logs"
$logFile   = Join-Path $logDir "watchdog.log"
$stateFile = Join-Path $logDir "watchdog.state"
$port      = 8787
$localUrl  = "http://127.0.0.1:$port/health"
$publicUrl = "https://retrigger-webhook.levilawson.net/health"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log([string]$Message) {
    if ((Test-Path $logFile) -and ((Get-Item $logFile).Length -gt 1MB)) {
        Move-Item -Force $logFile "$logFile.1"
    }
    $line = "{0}Z {1}" -f (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"), $Message
    Add-Content -Path $logFile -Value $line -Encoding UTF8
}

function Test-Health([string]$Url, [int]$TimeoutSec) {
    try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec
        return ($r.StatusCode -eq 200)
    } catch {
        return $false
    }
}

# Optional public check: log only when the state changes, and never restart anything for it.
function Update-PublicState {
    $state = "fail"
    if (Test-Health $publicUrl 10) { $state = "ok" }
    $prev = ""
    if (Test-Path $stateFile) { $prev = [string](Get-Content $stateFile -ErrorAction SilentlyContinue | Select-Object -First 1) }
    if ($prev -ne $state) {
        if ($state -eq "fail") {
            Write-Log "WARN public health failing while local health is OK: tunnel or DNS problem (the OI watchdog owns cloudflared; not restarting it)"
        } elseif ($prev -ne "") {
            Write-Log "public health recovered"
        }
        Set-Content -Path $stateFile -Value $state
    }
}

# Local check, with a second look 5 seconds later so one slow response does not trigger a restart.
$healthy = Test-Health $localUrl 3
if (-not $healthy) {
    Start-Sleep -Seconds 5
    $healthy = Test-Health $localUrl 3
}
if ($healthy) {
    Update-PublicState
    exit 0
}

Write-Log "local health check failed twice; restarting serve"

# 1. Stop stale serve processes (hung or half-started).
$killed = @()
Get-CimInstance Win32_Process -Filter "Name LIKE 'python%'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "fvdebtor_creditcheck\s+serve" } |
    ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        $killed += $_.ProcessId
    }

# 2. Free the port if a python process still holds it. Anything else is left alone.
$holders = netstat -ano | ForEach-Object {
    if ($_ -match "^\s*TCP\s+\S+:$port\s+\S+\s+LISTENING\s+(\d+)\s*$") { [int]$Matches[1] }
} | Sort-Object -Unique
foreach ($holder in $holders) {
    $p = Get-Process -Id $holder -ErrorAction SilentlyContinue
    if ($null -eq $p) { continue }
    if ($p.ProcessName -like "python*") {
        Stop-Process -Id $holder -Force -ErrorAction SilentlyContinue
        $killed += $holder
    } else {
        Write-Log "ERROR port $port is held by $($p.ProcessName) (pid $holder), not python; not touching it"
        exit 1
    }
}
if ($killed.Count -gt 0) {
    Write-Log ("stopped stale process(es): " + (($killed | Sort-Object -Unique) -join ", "))
    Start-Sleep -Seconds 2
}

# 3. Start serve hidden, with output in log files.
if (-not (Test-Path $python)) {
    Write-Log "ERROR python not found at $python"
    exit 1
}
$out = Join-Path $logDir "serve.out.log"
$err = Join-Path $logDir "serve.err.log"
if (Test-Path $out) { Move-Item -Force $out (Join-Path $logDir "serve.out.prev.log") }
if (Test-Path $err) { Move-Item -Force $err (Join-Path $logDir "serve.err.prev.log") }
Start-Process -FilePath $python -ArgumentList @("-m", "fvdebtor_creditcheck", "serve") `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput $out -RedirectStandardError $err | Out-Null

$ok = $false
$waited = 0
for ($i = 1; $i -le 15; $i++) {
    Start-Sleep -Seconds 1
    $waited = $i
    if (Test-Health $localUrl 3) { $ok = $true; break }
}
if ($ok) {
    Write-Log "restarted serve; healthy after $waited s"
    Update-PublicState
} else {
    Write-Log "ERROR serve was started but /health is still failing after 15 s; see logs\serve.err.log (is REPLAY_API_TOKEN set in .env?)"
    exit 1
}
