# FactorView expired credit reports -> Zapier

Reads expired debtor credit reports from MongoDB (`payments-service.fvDebtor`, query
`{"doc.creditReportExpired": true}`, read-only) and POSTs **one event per debtor** to a Zapier Catch Hook.
It never calls FactorView. Zapier later does the CreditSafe lookup, FactorView update and AE notification.

## Delivery semantics (read this)
* **At least once, not exactly once.** "Accepted" means Zapier returned 2xx (handed to Zapier), not that a
  credit refresh happened. An HTTP timeout can occur *after* Zapier accepted the event; the event is then
  retried/replayed and may arrive twice. `event_id` is deterministic (`sha256(debtor_id|report_date)`) and
  never changes, including on replay.
* **The Zap MUST check `event_id` in persistent storage (e.g. Zapier Storage/Table) before requesting a
  CreditSafe report, otherwise duplicate credit pulls can occur. Do this before go-live.**
* Dedup key = debtor ID + existing credit report date. A new report date that is still expired is a new event.
  Duplicate Mongo records for one key collapse to the one with the newest `updatedAt`.
* Accepted events are never resent by scheduled runs. Failed events are kept in SQLite and retried on later runs
  while Mongo still reports them expired.

## Setup (this VM, Linux)
```bash
cd /home/user/mongo_fvDebtor_CreditCheck
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt        # add requirements-dev.txt for tests
cp .env.example .env && chmod 600 .env           # then edit MONGO_URI and ZAPIER_WEBHOOK_URL
```
State: `state/deliveries.sqlite3` (UTC timestamps), lock `state/job.lock`, logs `logs/job.log` (rotating).
All are git-ignored. Logs never contain the Mongo URI, webhook URL, or raw Mongo documents.

## Commands
```bash
PY=.venv/bin/python
$PY -m pytest -q                                   # tests (mocked Mongo/webhook)
$PY -m fvdebtor_creditcheck run --limit 1          # DRY-RUN (default): reads Mongo, sends/writes nothing
$PY -m fvdebtor_creditcheck run --send --limit 1   # send exactly one unsent event
$PY -m fvdebtor_creditcheck run --send             # send all eligible unsent events
$PY -m fvdebtor_creditcheck status [--status failed|pending|accepted] [--limit N]
$PY -m fvdebtor_creditcheck replay <event_id>          # shows what would be replayed
$PY -m fvdebtor_creditcheck replay <event_id> --send   # resend same payload and event_id
```
`--limit` applies after removing already-accepted events, so sent rows cannot starve the backlog.
Run summary: `matched, duplicate, already_sent, eligible, selected, accepted, failed, invalid`.
Exit codes: 0 ok, 1 failures/abort, 2 config/usage, 3 Mongo error, 4 another run in progress.

## Retries and failures
* Retried (max 4 attempts, 2s/4s/8s backoff, capped): connection errors, timeouts, HTTP 408/429/5xx.
  `Retry-After` is honoured, capped at 60s. Timeouts: 5s connect / 20s read. Redirects are not followed.
* HTTP 401/403/404/410/3xx = destination configuration error: the run **stops** immediately.
* Other 4xx: not retried; event marked `failed` and kept; run continues. 3 consecutive failed events abort the run.
* Invalid debtor IDs or report dates are logged (`invalid record skipped for review`) and skipped; no date is substituted.
* Overlapping `--send`/replay runs are prevented by a file lock (dry-runs don't lock or write).

## Scheduling (NOT enabled; schedule time is a placeholder - confirm first)
Linux/systemd (intended zone America/Chicago; units in `deploy/`):
```bash
sudo cp deploy/fvdebtor-creditcheck.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fvdebtor-creditcheck.timer    # only after sign-off
systemctl list-timers fvdebtor-creditcheck.timer
```
Cron alternative: `deploy/crontab.example` (uses `CRON_TZ=America/Chicago`). The VM had no existing
scheduled-job convention for this project (empty repo, only system cron jobs), so these are new.
