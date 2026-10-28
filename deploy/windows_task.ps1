# Registers the scheduled task DISABLED. Review, then enable with: Enable-ScheduledTask -TaskName FvDebtorCreditCheck
# PLACEHOLDER schedule: weekdays 07:00. Task Scheduler uses the PC's clock, so the PC must be set to
# Central Time (America/Chicago), or adjust the time below.
$root = "C:\Users\paymentsuser\mongo_fvDebtor_CreditCheck"
$action = New-ScheduledTaskAction -Execute "$root\.venv\Scripts\python.exe" `
  -Argument "-m fvdebtor_creditcheck run --send --expired-within-days 90 --purchased-within-days 180" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 7:00AM
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName FvDebtorCreditCheck -Action $action -Trigger $trigger -Settings $settings -Disable
