# Registers the two weekday tasks for this project, both DISABLED so you can review them first:
#   "FV Credit Check Daily"     8:00 AM  run --send --expired-within-days 7 --purchased-within-days 180
#   "FV Credit Reconcile Daily" 9:00 AM  reconcile (records new scores from Mongo for events already sent)
# Task Scheduler uses this PC's clock: the PC must be on Central Time for 8:00 AM to mean Central (check: tzutil /g).
# Enable when ready:  Enable-ScheduledTask -TaskName "FV Credit Check Daily"; Enable-ScheduledTask -TaskName "FV Credit Reconcile Daily"
$root = "C:\Users\paymentsuser\mongo_fvDebtor_CreditCheck"
$py   = "$root\.venv\Scripts\python.exe"
$days = @("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

# Same account settings as the existing OI tasks when available.
$oi = Get-ScheduledTask -TaskName "OI Automation Watchdog" -ErrorAction SilentlyContinue
if ($oi) {
    $principal = $oi.Principal
} else {
    $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Limited
}
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

function Register-FvTask([string]$Name, [string]$Arguments, [string]$At) {
    $action  = New-ScheduledTaskAction -Execute $py -Argument $Arguments -WorkingDirectory $root
    $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At $At
    Register-ScheduledTask -TaskName $Name -Action $action -Trigger $trigger -Settings $settings `
        -Principal $principal -Force | Out-Null
    Disable-ScheduledTask -TaskName $Name | Out-Null
}

Register-FvTask "FV Credit Check Daily" "-m fvdebtor_creditcheck run --send --expired-within-days 7 --purchased-within-days 180" "8:00AM"
Register-FvTask "FV Credit Reconcile Daily" "-m fvdebtor_creditcheck reconcile" "9:00AM"

Get-ScheduledTask -TaskName "FV Credit Check Daily", "FV Credit Reconcile Daily" | Select-Object TaskName, State
