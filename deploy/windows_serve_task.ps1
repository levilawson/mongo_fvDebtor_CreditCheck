# Registers the replay endpoint to start at boot, DISABLED. Review, then:  Enable-ScheduledTask -TaskName FvDebtorReplayApi
# It listens on 127.0.0.1 only; the Cloudflare tunnel is what exposes it (see README).
$root = "C:\Users\paymentsuser\mongo_fvDebtor_CreditCheck"
$action = New-ScheduledTaskAction -Execute "$root\.venv\Scripts\python.exe" `
  -Argument "-m fvdebtor_creditcheck serve" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName FvDebtorReplayApi -Action $action -Trigger $trigger -Settings $settings -Disable
