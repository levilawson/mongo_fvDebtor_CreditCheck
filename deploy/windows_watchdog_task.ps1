# Registers the "FV Replay Watchdog" scheduled task: at startup, then every 5 minutes, running deploy\watchdog.ps1.
# It uses the same account settings as the existing "OI Automation Watchdog" task when that task exists.
# This task is the only thing that starts `serve`; the older FvDebtorReplayApi task is disabled below.
$root = "C:\Users\paymentsuser\mongo_fvDebtor_CreditCheck"
$name = "FV Replay Watchdog"

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$root\deploy\watchdog.ps1`"" `
    -WorkingDirectory $root

# Trigger 1: at startup, repeating every 5 minutes. Trigger 2: starts the same loop right now (no reboot needed).
$atStartup = New-ScheduledTaskTrigger -AtStartup
$repeat = (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)).Repetition
$atStartup.Repetition = $repeat
$now = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 4) `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

$oi = Get-ScheduledTask -TaskName "OI Automation Watchdog" -ErrorAction SilentlyContinue
if ($oi) {
    $principal = $oi.Principal
} else {
    $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Limited
}

Register-ScheduledTask -TaskName $name -Action $action -Trigger @($atStartup, $now) -Settings $settings `
    -Principal $principal -Force | Out-Null

# One thing starts serve: the watchdog.
Disable-ScheduledTask -TaskName "FvDebtorReplayApi" -ErrorAction SilentlyContinue | Out-Null

Get-ScheduledTask -TaskName $name, "FvDebtorReplayApi" -ErrorAction SilentlyContinue | Select-Object TaskName, State
