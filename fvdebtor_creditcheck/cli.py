"""Command line: run | status | replay."""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys

from .config import Config, ConfigError, load_config
from .events import utc_iso
from .mongo_source import MongoReadError, fetch_expired
from .runner import AlreadyRunning, replay_event, run_job, single_instance
from .store import Store
from .webhook import WebhookClient

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


def build_client(cfg: Config) -> WebhookClient:
    return WebhookClient(cfg.require_webhook(), cfg.connect_timeout, cfg.read_timeout,
                         cfg.max_attempts, cfg.backoff_base, cfg.retry_after_max)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fvdebtor_creditcheck", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="find expired reports and (with --send) post them to Zapier")
    r.add_argument("--send", action="store_true", help="actually POST to the webhook (default: dry-run)")
    r.add_argument("--limit", type=int, help="max eligible, unsent events to process")
    r.add_argument("--debtor-id", help="only process this debtor ID (e.g. for a single test)")
    s = sub.add_parser("status", help="show delivery status")
    s.add_argument("--status", choices=["pending", "failed", "accepted"])
    s.add_argument("--limit", type=int, default=50)
    rp = sub.add_parser("replay", help="resend a stored event with the same event_id")
    rp.add_argument("event_id")
    rp.add_argument("--send", action="store_true", help="actually POST (default: show what would be sent)")
    return p


def cmd_run(cfg: Config, args) -> int:
    if args.limit is not None and args.limit < 1:
        print("--limit must be >= 1", file=sys.stderr)
        return 2
    if args.send:
        cfg.require_webhook()
    docs = fetch_expired(cfg.require_mongo())
    if not args.send:
        store = Store(cfg.state_db_path, readonly=True)  # dry-run never writes
        try:
            summary = run_job(docs, store, None, False, args.limit, debtor_id=args.debtor_id)
        finally:
            store.close()
        print(f"[dry-run] would send debtor_ids: {summary.would_send}")
    else:
        with single_instance(cfg.lock_path):
            store = Store(cfg.state_db_path)
            try:
                summary = run_job(docs, store, build_client(cfg), True, args.limit,
                                  cfg.max_consecutive_failed_events, debtor_id=args.debtor_id)
            finally:
                store.close()
    log.info("run summary: %s", summary.line())
    print(summary.line())
    return 1 if (summary.failed or summary.aborted) else 0


def cmd_status(cfg: Config, args) -> int:
    store = Store(cfg.state_db_path, readonly=True)
    try:
        print("counts:", store.counts() or "{}")
        for r in store.list(args.status, args.limit):
            print(f"{r['event_id']}  debtor={r['debtor_id']}  report_date={r['report_date']}  "
                  f"status={r['status']}  attempts={r['attempts']}  last_attempt={r['last_attempt_at']}  "
                  f"http={r['last_http_status']}  error={r['last_error']}")
    finally:
        store.close()
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
    with single_instance(cfg.lock_path):
        store = Store(cfg.state_db_path)
        try:
            summary = replay_event(store, build_client(cfg), args.event_id)
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
        return {"run": cmd_run, "status": cmd_status, "replay": cmd_replay}[args.cmd](cfg, args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except MongoReadError as exc:
        log.error("mongo read failed: %s", exc)
        return 3
    except AlreadyRunning:
        log.warning("another run is in progress; exiting")
        return 4
