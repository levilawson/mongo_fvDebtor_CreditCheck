"""Command line: run | status | replay."""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import logging.handlers
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config, ConfigError, load_config
from .events import utc_iso
from .mongo_source import (MongoReadError, SuspiciousActivityData, fetch_active_debtors, fetch_companies,
                           fetch_current_ratings, fetch_debtor_clients, fetch_expired)
from .runner import AlreadyRunning, reconcile, replay_event, run_job, single_instance
from .store import Store
from .webhook import RoutedWebhook, WebhookClient

log = logging.getLogger("fvdebtor_creditcheck")


def setup_logging(cfg: Config) -> None:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    fmt.converter = __import__("time").gmtime
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.handlers.RotatingFileHandler(cfg.log_dir / "job.log", maxBytes=2_000_000, backupCount=5),
              logging.StreamHandler(sys.stderr)):
        h.setFormatter(fmt)
        root.addHandler(h)
    logging.getLogger("urllib3").setLevel(logging.WARNING)  # avoid URL-bearing debug lines


def build_client(cfg: Config) -> RoutedWebhook:
    def one(url):
        return WebhookClient(url, cfg.connect_timeout, cfg.read_timeout,
                             cfg.max_attempts, cfg.backoff_base, cfg.retry_after_max)
    return RoutedWebhook(one(cfg.require_webhook()), one(cfg.require_inactive_webhook()))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fvdebtor_creditcheck", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="find expired reports and (with --send) post them to Zapier")
    r.add_argument("--send", action="store_true", help="actually POST to the webhook (default: dry-run)")
    r.add_argument("--limit", type=int, help="max eligible, unsent events to process")
    r.add_argument("--show-payload", action="store_true",
                   help="dry-run only: print the JSON payload(s) that would be sent")
    r.add_argument("--expired-within-days", type=int, metavar="N",
                   help="only reports that expired (report date + REPORT_VALIDITY_DAYS) within the last N days")
    r.add_argument("--purchased-within-days", type=int, metavar="N",
                   help="only debtors with an invoice purchased in the last N days; others are skipped "
                        "(not sent to the inactive webhook)")
    r.add_argument("--debtor-id", help="only process this debtor ID (e.g. for a single test)")
    s = sub.add_parser("status", help="show delivery status")
    s.add_argument("--status", choices=["pending", "failed", "accepted"])
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--unresolved", action="store_true",
                   help="accepted events with no newer credit report seen in Mongo yet")
    h = sub.add_parser("history", help="all stored events, delivery attempts and scores for one debtor")
    h.add_argument("debtor_id")
    rn = sub.add_parser("runs", help="recent job runs (sends, replays, reconciles)")
    rn.add_argument("--limit", type=int, default=20)
    ex = sub.add_parser("export", help="write all events with old/new scores to a CSV file")
    ex.add_argument("--out", help="CSV path (default: state/export_<UTC date>.csv)")
    bk = sub.add_parser("backup", help="consistent copy of the SQLite database")
    bk.add_argument("--out", help="path (default: state/backup_<UTC timestamp>.sqlite3)")
    sub.add_parser("reconcile", help="record new scores for accepted events once Mongo shows a newer credit report")
    rp = sub.add_parser("replay", help="resend a stored event with the same event_id")
    rp.add_argument("event_id")
    rp.add_argument("--send", action="store_true", help="actually POST (default: show what would be sent)")
    return p


def _summary_dict(summary) -> dict:
    d = dataclasses.asdict(summary)
    d.pop("would_send", None)
    d.pop("previews", None)
    return d


def cmd_run(cfg: Config, args) -> int:
    for name in ("limit", "expired_within_days", "purchased_within_days"):
        v = getattr(args, name)
        if v is not None and v < 1:
            print(f"--{name.replace('_', '-')} must be >= 1", file=sys.stderr)
            return 2
    if args.send:
        cfg.require_webhook()
        cfg.require_inactive_webhook()
    docs = fetch_expired(cfg.require_mongo())
    lookup = lambda ids: fetch_companies(cfg.require_mongo(), ids)  # noqa: E731
    window = args.purchased_within_days or cfg.activity_window_days
    since = (datetime.now(timezone.utc).date() - timedelta(days=window)).isoformat()
    active = lambda: fetch_active_debtors(cfg.require_mongo(), since)  # noqa: E731
    linked = lambda ids: fetch_debtor_clients(cfg.require_mongo(), ids)  # noqa: E731
    if not args.send:
        store = Store(cfg.state_db_path, readonly=True)  # dry-run never writes
        try:
            summary = run_job(docs, store, None, False, args.limit, debtor_id=args.debtor_id,
                              company_lookup=lookup, debtor_clients_lookup=linked,
                              active_lookup=active, window_days=window,
                              expired_within_days=args.expired_within_days,
                              skip_inactive=bool(args.purchased_within_days),
                              validity_days=cfg.report_validity_days)
        finally:
            store.close()
        print(f"[dry-run] would send debtor_ids: {summary.would_send}")
        if args.show_payload:
            for payload in summary.previews:
                print(json.dumps(payload, indent=2))
        if args.debtor_id and not summary.would_send:
            print(f"note: no eligible event for debtor id {args.debtor_id!r} "
                  "(not expired, already sent, or the ID was mistyped)")
    else:
        with single_instance(cfg.lock_path):
            store = Store(cfg.state_db_path)
            store.start_run("run --send", {"limit": args.limit, "debtor_id": args.debtor_id,
                                           "expired_within_days": args.expired_within_days,
                                           "purchased_within_days": args.purchased_within_days})
            try:
                summary = run_job(docs, store, build_client(cfg), True, args.limit,
                                  cfg.max_consecutive_failed_events, debtor_id=args.debtor_id,
                                  company_lookup=lookup, debtor_clients_lookup=linked,
                                  active_lookup=active, window_days=window,
                                  expired_within_days=args.expired_within_days,
                                  skip_inactive=bool(args.purchased_within_days),
                                  validity_days=cfg.report_validity_days)
                store.finish_run(_summary_dict(summary), summary.aborted)
            finally:
                store.close()
    log.info("run summary: %s", summary.line())
    print(summary.line())
    return 1 if (summary.failed or summary.aborted) else 0


def cmd_status(cfg: Config, args) -> int:
    store = Store(cfg.state_db_path, readonly=True)
    try:
        print("counts:", store.counts() or "{}")
        rows = store.list(args.status, args.limit)
        if args.unresolved:
            rows = store.unresolved_accepted()[:args.limit]
        for r in rows:
            k = r.keys()
            old = json.loads(r["payload_json"]).get("credit_score")
            new = r["new_score"] if "new_score" in k else None
            resolved = r["resolved_at"] if "resolved_at" in k else None
            print(f"{r['event_id']}  debtor={r['debtor_id']}  report_date={r['report_date']}  "
                  f"status={r['status']}  attempts={r['attempts']}  last_attempt={r['last_attempt_at']}  "
                  f"http={r['last_http_status']}  error={r['last_error']}  "
                  f"score={old}->{new if resolved else '?'}  resolved_at={resolved}")
    finally:
        store.close()
    return 0


def cmd_runs(cfg: Config, args) -> int:
    store = Store(cfg.state_db_path)
    try:
        for r in store.runs(args.limit):
            s = json.loads(r["summary_json"]) if r["summary_json"] else {}
            counts = " ".join(f"{k}={v}" for k, v in s.items() if k != "dry_run" and v not in (None, 0, False))
            print(f"#{r['run_id']}  {r['started_at']} -> {r['finished_at'] or 'unfinished'}  {r['command']}  "
                  f"{r['options_json']}  {counts}" + (f"  ABORTED={r['aborted']}" if r["aborted"] else ""))
    finally:
        store.close()
    return 0


def cmd_history(cfg: Config, args) -> int:
    store = Store(cfg.state_db_path)
    try:
        h = store.debtor_history(args.debtor_id)
        print(f"debtor {args.debtor_id}: {len(h['events'])} event(s)")
        for r in h["events"]:
            print(f"  event {r['event_id']} report_date={r['report_date']} status={r['status']} "
                  f"first_seen={r['first_seen_at']} accepted_at={r['accepted_at']} resolved_at={r['resolved_at']}")
        print("scores observed:")
        for r in h["scores"]:
            print(f"  {r['observed_at']}  report_date={r['report_date']}  score={r['score']}  ({r['source']})")
        print("delivery attempts:")
        for r in h["attempts"]:
            print(f"  {r['attempted_at']}  run=#{r['run_id']}  {r['outcome']}  http={r['http_status']}  "
                  f"tries={r['tries']}  error={r['error']}")
    finally:
        store.close()
    return 0


EXPORT_COLUMNS = ["event_id", "debtor_id", "debtor_name", "event_type", "report_date", "expires_on", "old_score",
                  "new_score", "new_report_date", "status", "attempts", "first_seen_at", "accepted_at",
                  "resolved_at", "last_http_status", "last_error", "client_count", "clients"]


def cmd_export(cfg: Config, args) -> int:
    out = Path(args.out) if args.out else cfg.state_db_path.parent / f"export_{utc_iso()[:10]}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    store = Store(cfg.state_db_path)
    try:
        rows = store.conn.execute("SELECT * FROM events ORDER BY first_seen_at").fetchall()
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=EXPORT_COLUMNS)
            w.writeheader()
            for r in rows:
                p = json.loads(r["payload_json"])
                w.writerow({
                    "event_id": r["event_id"], "debtor_id": r["debtor_id"], "debtor_name": p.get("debtor_name"),
                    "event_type": p.get("event_type"), "report_date": r["report_date"],
                    "expires_on": p.get("credit_report_expires_on"), "old_score": p.get("credit_score"),
                    "new_score": r["new_score"], "new_report_date": r["new_report_date"], "status": r["status"],
                    "attempts": r["attempts"], "first_seen_at": r["first_seen_at"], "accepted_at": r["accepted_at"],
                    "resolved_at": r["resolved_at"], "last_http_status": r["last_http_status"],
                    "last_error": r["last_error"], "client_count": p.get("client_count"),
                    "clients": "; ".join(f"{c['client_id']} {c['client_name']} ({c['account_rep']})"
                                         for c in p.get("clients", []))})
    finally:
        store.close()
    print(f"wrote {len(rows)} event(s) to {out}")
    return 0


def cmd_backup(cfg: Config, args) -> int:
    out = Path(args.out) if args.out else cfg.state_db_path.parent / f"backup_{utc_iso().replace(':', '')}.sqlite3"
    store = Store(cfg.state_db_path)
    try:
        store.backup_to(out)
    finally:
        store.close()
    print(f"backup written to {out}")
    return 0


def cmd_reconcile(cfg: Config, args) -> int:
    ratings = lambda ids: fetch_current_ratings(cfg.require_mongo(), ids)  # noqa: E731
    with single_instance(cfg.lock_path):
        store = Store(cfg.state_db_path)
        store.start_run("reconcile", {})
        try:
            summary = reconcile(store, ratings, cfg.reconcile_stale_days)
            store.finish_run(_summary_dict(summary))
        finally:
            store.close()
    log.info("reconcile summary: %s", summary.line())
    print(summary.line())
    return 0


def cmd_replay(cfg: Config, args) -> int:
    if not args.send:
        store = Store(cfg.state_db_path, readonly=True)
        row = store.get_by_event_id(args.event_id)
        store.close()
        if row is None:
            print("unknown event_id", file=sys.stderr)
            return 2
        print(f"[dry-run] would replay event_id={row['event_id']} debtor={row['debtor_id']} "
              f"report_date={row['report_date']} status={row['status']}; add --send to transmit")
        return 0
    cfg.require_webhook()
    cfg.require_inactive_webhook()
    with single_instance(cfg.lock_path):
        store = Store(cfg.state_db_path)
        store.start_run("replay --send", {"event_id": args.event_id})
        try:
            summary = replay_event(store, build_client(cfg), args.event_id)
            store.finish_run(_summary_dict(summary), summary.aborted)
        except KeyError:
            print("unknown event_id", file=sys.stderr)
            return 2
        finally:
            store.close()
    print(summary.line())
    return 1 if summary.failed else 0


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        cfg = load_config()
        setup_logging(cfg)
        return {"run": cmd_run, "status": cmd_status, "replay": cmd_replay,
                "reconcile": cmd_reconcile, "history": cmd_history, "runs": cmd_runs,
                "export": cmd_export, "backup": cmd_backup}[args.cmd](cfg, args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except MongoReadError as exc:
        log.error("mongo read failed: %s", exc)
        return 3
    except SuspiciousActivityData as exc:
        log.error("activity check aborted the run: %s", exc)
        return 3
    except AlreadyRunning:
        log.warning("another run is in progress; exiting")
        return 4
