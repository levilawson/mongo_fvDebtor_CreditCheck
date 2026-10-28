# Registers the replay endpoint to start when you log on, DISABLED. Review, then:
#   Enable-ScheduledTask -TaskName FvDebtorReplayApi ; Start-ScheduledTask -TaskName FvDebtorReplayApi
# It listens on 127.0.0.1 only; the Cloudflare tunnel is what exposes it (see README).
# To start at boot without a login instead, copy the principal/trigger settings of your existing
# "OI Cloudflare Tunnel" task (Get-ScheduledTask "OI Cloudflare Tunnel" | Format-List *).
$root = "C:\Users\paymentsuser\mongo_fvDebtor_CreditCheck"
$action = New-ScheduledTaskAction -Execute "$root\.venv\Scripts\python.exe" `
  -Argument "-m fvdebtor_creditcheck serve" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable
Register-ScheduledTask -TaskName FvDebtorReplayApi -Action $action -Trigger $trigger -Settings $settings | Out-Null
Disable-ScheduledTask -TaskName FvDebtorReplayApi | Out-Null
Get-ScheduledTask -TaskName FvDebtorReplayApi | Select-Object TaskName, State
