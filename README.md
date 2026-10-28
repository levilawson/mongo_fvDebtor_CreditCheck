# FactorView expired credit reports -> Zapier

Reads expired debtor credit reports from MongoDB (`payments-service.fvDebtor`, query
`{"doc.creditReportExpired": true}`, read-only) and POSTs **one event per debtor** to a Zapier Catch Hook.
It never calls FactorView. Zapier later does the CreditSafe lookup, FactorView update and AE notification.

## Payload: clients and account reps
Each debtor event includes `clients`: every `clientId` linked to the debtor in `fvDebtor` (looked up by debtor ID,
regardless of each record's expired flag), resolved against `fvCompany` by `doc.id`:
`{"client_id", "client_name", "client_label", "account_rep"}`, where `client_label` is the ID and name on one line
(`Name (FV ID: 1085)`). Each `rep_groups` entry also has `clients_lines` (one label per line) next to `clients_text`. `rep_groups` regroups the same clients per account rep (null rep last) with a ready-made `clients_text`
("Name (FV ID: 1), Name (FV ID: 2), and Name (FV ID: 3)") so the Zap can loop over reps and send one message each.
Clients whose `fvCompany` name starts with `Z -` (FactorView's commented-out convention) or with the word `Test`
("Test Company"; not "Testa Energy") are excluded, plus anything matching the optional `CLIENT_EXCLUDE_REGEX`
(`clients_excluded` in the summary). `client_count` is the number of clients after exclusions. An event with `client_count: 0` is still sent
(the Zap routes it to the AE manager). Debtors are never filtered by name. Unresolved clients are sent with null name/rep and counted in
`clients_unresolved`. Rep data is refreshed on every run until the event is accepted; after acceptance the event is
never resent, so later rep or client changes are not re-announced.

## Activity routing (second webhook)
A debtor is **active** if `fvInvoice` has at least one document with `doc.isPurchase: true` and
`doc.buyDate >= today - 180 days` (UTC; `ACTIVITY_WINDOW_DAYS` to change; one indexed read-only query).
* Active -> `ZAPIER_WEBHOOK_URL`, `event_type` `factorview.credit_report_expired`, `is_active: true`.
* Inactive -> `ZAPIER_INACTIVE_WEBHOOK_URL`, `event_type` `factorview.credit_report_expired_inactive`,
  `is_active: false`, plus `inactive_reason` and `inactivity_window_days`. Same client/rep fields. The Zap should skip
  CreditSafe and message the reps "no invoices purchased in 180 days or more".
* The route is part of the dedup key: an inactive event has its own `event_id`, so a debtor that later becomes active
  (same report date) still sends one active event. Each route is sent at most once per report date.
* If the query finds no purchases at all in the window (wrong field/format), the run aborts instead of routing everyone
  to the inactive hook. `--send` and `replay --send` need both webhook URLs set.

## Tracking the new score (`reconcile`)
Zapier doesn't write back to this PC. The Zap updates FactorView, Mongo syncs it back, and `reconcile` (read-only on Mongo)
records the result in SQLite: for each accepted active-route event with no result yet, if any `fvDebtor` document for the
debtor now has a credit report date **newer** than the one we sent, it stores `new_score`, `new_report_date` and
`resolved_at` (the newest report wins). A new report that is itself expired later is a separate event. Inactive-route
events are not reconciled (no refresh is expected). Events still unresolved after `RECONCILE_STALE_DAYS` (default 3) are
logged as warnings and counted as `stale`. Existing databases are migrated automatically.
```powershell
& $PY -m fvdebtor_creditcheck reconcile                 # summary: checked, resolved, unresolved, stale
& $PY -m fvdebtor_creditcheck status                    # shows score=old->new and resolved_at
& $PY -m fvdebtor_creditcheck status --unresolved       # accepted events still waiting for a new report
```

## Rollout cohort filters
`credit_report_date` (FactorView `creditRating.date`) is when the rating was last updated. A report expires
`REPORT_VALIDITY_DAYS` later (default 30), shown in the payload as `credit_report_expires_on`. Two optional `run` flags
narrow the first rollout (applied before dedup and before `--limit`, so the limit counts only cohort events):
* `--expired-within-days N`: only reports whose expiry date (report date + validity) falls in the last N days (not in the
  future). 90 is used for the rollout so long-dormant debtors (years past expiry) are ignored.
* `--purchased-within-days N`: only debtors with an invoice purchased in the last N days. Other debtors are
  **skipped** (counted as `skipped_inactive`), not sent to the inactive webhook, and nothing is recorded for them,
  so widening or removing the flags later picks them up automatically.
```powershell
& $PY -m fvdebtor_creditcheck run --expired-within-days 90 --purchased-within-days 180            # dry-run
& $PY -m fvdebtor_creditcheck run --expired-within-days 90 --purchased-within-days 180 --limit 1 --send
```
Without these flags the full routing above applies (inactive debtors go to the second webhook).

## Local history (everything stays on this PC)
`state\deliveries.sqlite3` holds: `events` (latest state per event, with old/new score), `runs` (every send, replay and
reconcile with its options and summary; dry-runs write nothing), `attempts` (every delivery attempt with HTTP status and
error), and `score_history` (the score at detection, source `detected`, and the refreshed score once Mongo shows a newer
report, source `refreshed`). Databases created before these tables are upgraded and back-filled automatically.
```powershell
& $PY -m fvdebtor_creditcheck runs --limit 20          # recent runs
& $PY -m fvdebtor_creditcheck history 1892             # events, scores over time and attempts for one debtor
& $PY -m fvdebtor_creditcheck export --out C:\path\events.csv   # all events with old/new scores and clients (CSV)
& $PY -m fvdebtor_creditcheck backup                   # consistent copy to state\backup_<timestamp>.sqlite3
```
Back up regularly (the backup command is safe while the job is idle) and keep copies off this PC: the database is the only
record of what was sent, and losing it would let the job resend. Exports contain debtor/client/rep names; `*.csv` and
`state/` are git-ignored.

## Rerunning one event from Zapier (no inbound endpoint)
Every payload carries `event_id` and `payload_json` (the complete event as one JSON string). When a debtor has no
CreditSafe ID, the Zap saves `event_id`, `debtor_id` and `payload_json` in a "pending reruns" Zapier Table. A rerun button
(Zapier Interface/Table) starts a second Zap with that `event_id`; it loads the saved row, checks the mapping row now
exists, and continues from the CreditSafe step using the stored payload. Nothing on this PC is exposed.

## Rerun one event from a Zap (`serve`, behind your Cloudflare tunnel)
`serve` is a small authenticated HTTP endpoint on this PC. A Zap (for example a "Resend" button after a CreditSafe ID is
added) calls it with an `event_id`; the job rebuilds that event from current Mongo data (same `event_id` and
`detected_at`; falls back to the stored payload if Mongo no longer matches) and posts it to the Zapier hook again.
* `POST /replay`, header `Authorization: Bearer <REPLAY_API_TOKEN>` (or `X-Webhook-Secret: <REPLAY_API_TOKEN>`), body `{"event_id": "fvcr-...", "refresh": true}`.
  Responses: 200 `accepted` (or 502 `failed`), 400 bad id, 401 bad token, 404 unknown event, 409 another run in
  progress, 429 same event was attempted within `REPLAY_COOLDOWN_SECONDS` (default 300; stops double clicks).
  `GET /health` returns `{"ok": true}` without auth.
* It binds to loopback only; set `REPLAY_API_TOKEN` (32+ random chars) in `.env`; the token and URLs are never logged.
* Zapier's webhook step times out after about 30 seconds; a replay normally takes a few seconds (one Mongo read plus one
  POST), but a failing destination with retries can take longer. Treat a timeout as unknown and check `history`.
Setup on this PC:
```powershell
& $PY -c "import secrets; print(secrets.token_urlsafe(48))"      # put the output in .env as REPLAY_API_TOKEN
& $PY -m fvdebtor_creditcheck serve                                # test locally, Ctrl+C to stop
curl http://127.0.0.1:8787/health
```
Tunnel: add an ingress rule to your existing tunnel config pointing a hostname at `http://127.0.0.1:8787` (example in
`deploy/cloudflared-ingress.example.yml`), then `cloudflared tunnel route dns <tunnel> <hostname>` and restart
cloudflared. Strongly recommended: put Cloudflare Access (a service-token policy) in front of that hostname and have the
Zap send the `CF-Access-Client-Id` / `CF-Access-Client-Secret` headers as well as the bearer token.
Zap step: Webhooks by Zapier, Custom Request, POST `https://<hostname>/replay`, headers `Authorization: Bearer <token>`
and `Content-Type: application/json`, body `{"event_id": "<event_id>"}`.
To keep it running, the **FV Replay Watchdog** scheduled task (`deploy\\windows_watchdog_task.ps1`) runs `deploy\\watchdog.ps1`
at startup and every 5 minutes. The script checks `http://127.0.0.1:8787/health` (two looks, 5 seconds apart); if it is
down it stops stale `serve` processes, frees the port (python processes only), starts `serve` hidden with output in
`logs\\serve.out.log` / `logs\\serve.err.log`, and logs each restart to `logs\\watchdog.log`. If local health is fine but the
public URL fails it only logs a warning: the tunnel belongs to the OI watchdog and this project never starts or stops
cloudflared. The watchdog is the only thing that should start `serve` (the older FvDebtorReplayApi task is disabled).
`serve` is threaded, so `/health` still answers during a slow replay.
Because the Zap has no `event_id` check, the Zap itself must still stop a second press (for example a "Needs Rerun" flag).

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

## Setup (Windows PC, PowerShell)
```powershell
cd C:\Users\paymentsuser
git clone -b claude/focused-brown-cyc8rx https://github.com/levilawson/mongo_fvDebtor_CreditCheck.git
cd mongo_fvDebtor_CreditCheck
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt     # add requirements-dev.txt for tests
copy .env.example .env                                 # then edit .env with MONGO_URI and ZAPIER_WEBHOOK_URL
```
State: `state\deliveries.sqlite3` (UTC timestamps), lock `state\job.lock`, logs `logs\job.log` (rotating).
All are git-ignored. Logs never contain the Mongo URI, webhook URL, or raw Mongo documents.
(On Linux use `.venv/bin/python` instead of `.venv\Scripts\python.exe`.)

## Commands
```powershell
$PY = ".\.venv\Scripts\python.exe"
& $PY -m pytest -q                                   # tests (mocked Mongo/webhook)
& $PY -m fvdebtor_creditcheck run --limit 1          # DRY-RUN (default): reads Mongo, sends/writes nothing
& $PY -m fvdebtor_creditcheck run --debtor-id 2303 --send   # send one specific debtor (dry-run without --send) $PY -m fvdebtor_creditcheck run --debtor-id 1892 --show-payload   # dry-run that prints the JSON payload
& $PY -m fvdebtor_creditcheck run --debtor-id 2303 --send   # send one specific debtor (dry-run without --send)
& $PY -m fvdebtor_creditcheck run --send --limit 1   # send exactly one unsent event
& $PY -m fvdebtor_creditcheck run --send             # send all eligible unsent events
& $PY -m fvdebtor_creditcheck status [--status failed|pending|accepted] [--limit N]
& $PY -m fvdebtor_creditcheck replay <event_id>          # shows what would be replayed
& $PY -m fvdebtor_creditcheck replay <event_id> --send   # resend same payload and event_id
& $PY -m fvdebtor_creditcheck replay --debtor-id 1380 --refresh --show-payload   # dry-run: rebuild from current Mongo data
& $PY -m fvdebtor_creditcheck replay --debtor-id 1380 --refresh --send           # resend a refreshed payload, same event_id
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
`deploy\windows_task.ps1` registers a Task Scheduler task named `FvDebtorCreditCheck` **disabled**
(weekdays 07:00, overlapping starts ignored). Task Scheduler uses the PC clock, so set the PC to Central Time
for America/Chicago. Run from an elevated or normal PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File deploy\windows_task.ps1
Enable-ScheduledTask -TaskName FvDebtorCreditCheck      # only after dry-run, one-event send and Zap sign-off
```
