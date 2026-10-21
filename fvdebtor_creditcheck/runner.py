"""Orchestration: Mongo -> dedupe -> SQLite -> webhook."""
from __future__ import annotations

import json
import sys
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from . import webhook
from .events import collect_candidates, utc_iso
from .store import Store

log = logging.getLogger(__name__)


class AlreadyRunning(Exception):
    pass


@contextmanager
def single_instance(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w")
    try:
        try:
            if sys.platform == "win32":
                import msvcrt
                fh.write(" ")
                fh.flush()
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise AlreadyRunning("another run holds the lock") from None
        yield
    finally:
        fh.close()  # releases the flock


@dataclass
class Summary:
    matched: int = 0
    duplicate: int = 0
    already_sent: int = 0
    eligible: int = 0
    selected: int = 0
    accepted: int = 0
    failed: int = 0
    invalid: int = 0
    aborted: str | None = None
    dry_run: bool = True
    would_send: list[str] = field(default_factory=list)

    def line(self) -> str:
        s = (f"matched={self.matched} duplicate={self.duplicate} already_sent={self.already_sent} "
             f"eligible={self.eligible} selected={self.selected} accepted={self.accepted} "
             f"failed={self.failed} invalid={self.invalid} mode={'dry-run' if self.dry_run else 'send'}")
        return s + (f" ABORTED={self.aborted}" if self.aborted else "")


def _deliver(store: Store, client, row, summary: Summary) -> webhook.Result:
    payload = json.loads(row["payload_json"])
    res = client.send(payload)
    ok = res.outcome == webhook.ACCEPTED
    store.record_attempt(row["event_key"], ok, res.http_status, res.error, res.attempts)
    if ok:
        summary.accepted += 1
        log.info("accepted event_id=%s debtor_id=%s http=%s", row["event_id"], row["debtor_id"], res.http_status)
    else:
        summary.failed += 1
        log.error("failed event_id=%s debtor_id=%s http=%s error=%s",
                  row["event_id"], row["debtor_id"], res.http_status, res.error)
    return res


def run_job(docs, store: Store, client, send: bool, limit: int | None,
            max_consecutive_failed: int = 3) -> Summary:
    summary = Summary(dry_run=not send)
    collected = collect_candidates(docs)
    summary.matched, summary.duplicate = collected.matched, collected.duplicate
    summary.invalid = len(collected.invalid)
    for item in collected.invalid:
        log.warning("invalid record skipped for review: %s", item)

    # Filter out accepted events BEFORE applying the limit so they cannot starve the backlog.
    eligible = []
    for cand in collected.candidates:
        row = store.get(cand.event_key)
        if row is not None and row["status"] == "accepted":
            summary.already_sent += 1
        else:
            eligible.append((cand, row))
    summary.eligible = len(eligible)
    selected = eligible if limit is None else eligible[:max(limit, 0)]
    summary.selected = len(selected)

    if not send:
        summary.would_send = [c.debtor_id for c, _ in selected]
        return summary

    detected_at = utc_iso()
    consecutive = 0
    for cand, row in selected:
        row = row or store.ensure(cand.event_key, cand.build_payload(detected_at))
        res = _deliver(store, client, row, summary)
        if res.outcome == webhook.CONFIG_ERROR:
            summary.aborted = f"destination configuration error (HTTP {res.http_status})"
            break
        consecutive = 0 if res.outcome == webhook.ACCEPTED else consecutive + 1
        if consecutive >= max_consecutive_failed:
            summary.aborted = f"{consecutive} consecutive failed events"
            break
    return summary


def replay_event(store: Store, client, event_id: str) -> Summary:
    row = store.get_by_event_id(event_id)
    summary = Summary(dry_run=False)
    if row is None:
        raise KeyError(event_id)
    res = _deliver(store, client, row, summary)
    if res.outcome == webhook.CONFIG_ERROR:
        summary.aborted = f"destination configuration error (HTTP {res.http_status})"
    return summary
