"""Orchestration: Mongo -> dedupe -> SQLite -> webhook."""
from __future__ import annotations

import json
import sys
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from . import webhook
from datetime import date, datetime, timedelta, timezone

from .events import (InvalidRecord, _updated_sort_key, collect_candidates, is_excluded_client,
                     parse_report_date, utc_iso)
from .store import Store

log = logging.getLogger(__name__)


class AlreadyRunning(Exception):
    pass


@contextmanager
def single_instance(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "ab")  # never write to it: on Windows a buffered write into a locked byte fails on close
    try:
        try:
            if sys.platform == "win32":
                import msvcrt
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
    clients: int = 0
    clients_unresolved: int = 0
    clients_excluded: int = 0
    inactive: int = 0
    skipped_expired_window: int = 0
    skipped_inactive: int = 0
    aborted: str | None = None
    dry_run: bool = True
    would_send: list[str] = field(default_factory=list)
    previews: list[dict] = field(default_factory=list)

    def line(self) -> str:
        s = (f"matched={self.matched} duplicate={self.duplicate} already_sent={self.already_sent} "
             f"eligible={self.eligible} selected={self.selected} accepted={self.accepted} "
             f"failed={self.failed} invalid={self.invalid} clients={self.clients} "
             f"clients_unresolved={self.clients_unresolved} clients_excluded={self.clients_excluded} inactive={self.inactive} skipped_expired_window={self.skipped_expired_window} skipped_inactive={self.skipped_inactive} mode={'dry-run' if self.dry_run else 'send'}")
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
            max_consecutive_failed: int = 3, debtor_id: str | None = None,
            company_lookup=None, debtor_clients_lookup=None, active_lookup=None,
            window_days: int = 180, expired_within_days: int | None = None, skip_inactive: bool = False,
            today: date | None = None, validity_days: int = 30, exclude_regex=None) -> Summary:
    summary = Summary(dry_run=not send)
    collected = collect_candidates(docs)
    summary.matched, summary.duplicate = collected.matched, collected.duplicate
    summary.invalid = len(collected.invalid)
    for item in collected.invalid:
        log.warning("invalid record skipped for review: %s", item)

    if active_lookup:
        active = active_lookup()
        for cand in collected.candidates:
            cand.is_active = cand.debtor_id in active

    today = today or datetime.now(timezone.utc).date()

    # Filter out accepted events BEFORE applying the limit so they cannot starve the backlog.
    eligible = []
    for cand in collected.candidates:
        if debtor_id is not None and cand.debtor_id != debtor_id:
            continue
        if expired_within_days is not None:
            # creditRating.date is when the rating was last updated; it expires validity_days later.
            expires = date.fromisoformat(cand.report_date) + timedelta(days=validity_days)
            if not (today - timedelta(days=expired_within_days) <= expires <= today):
                summary.skipped_expired_window += 1
                continue
        if skip_inactive and not cand.is_active:
            summary.skipped_inactive += 1
            continue
        row = store.get(cand.store_key())
        if row is not None and row["status"] == "accepted":
            summary.already_sent += 1
        else:
            eligible.append((cand, row))
    summary.eligible = len(eligible)
    selected = eligible if limit is None else eligible[:max(limit, 0)]
    summary.selected = len(selected)
    summary.inactive = sum(1 for c, _ in selected if not c.is_active)

    if debtor_clients_lookup and selected:
        linked = debtor_clients_lookup({cand.debtor_id for cand, _ in selected})
        for cand, _ in selected:
            cand.client_ids |= linked.get(cand.debtor_id, set())

    all_ids = {c for cand, _ in selected for c in cand.client_ids}
    companies = company_lookup(all_ids) if company_lookup and all_ids else {}
    excluded = {c for c in all_ids if is_excluded_client((companies.get(c) or {}).get("name"), exclude_regex)}
    for cand, _ in selected:
        cand.client_ids -= excluded
    all_ids -= excluded
    summary.clients_excluded = len(excluded)
    summary.clients = len(all_ids)
    summary.clients_unresolved = sum(1 for c in all_ids if not (companies.get(c) or {}).get("name"))
    if company_lookup and summary.clients_unresolved:
        log.warning("%d client id(s) not found in fvCompany; sent with null name/rep", summary.clients_unresolved)

    if not send:
        summary.would_send = [c.debtor_id for c, _ in selected]
        summary.previews = [c.build_payload(utc_iso(), companies, window_days, validity_days) for c, _ in selected]
        return summary

    detected_at = utc_iso()
    consecutive = 0
    for cand, row in selected:
        payload = cand.build_payload(detected_at, companies, window_days, validity_days)
        key = cand.store_key()
        row = store.refresh_payload(key, payload) if row else store.ensure(key, payload)
        res = _deliver(store, client, row, summary)
        if res.outcome == webhook.CONFIG_ERROR:
            summary.aborted = f"destination configuration error (HTTP {res.http_status})"
            break
        consecutive = 0 if res.outcome == webhook.ACCEPTED else consecutive + 1
        if consecutive >= max_consecutive_failed:
            summary.aborted = f"{consecutive} consecutive failed events"
            break
    return summary


@dataclass
class ReconcileSummary:
    checked: int = 0
    resolved: int = 0
    unresolved: int = 0
    stale: int = 0

    def line(self) -> str:
        return (f"checked={self.checked} resolved={self.resolved} unresolved={self.unresolved} "
                f"stale={self.stale}")


def _newest_rating(docs: list[dict]):
    best = None
    for d in docs:
        try:
            date_ = parse_report_date(d.get("date"))
        except InvalidRecord:
            continue
        key = (date_, _updated_sort_key(d.get("updatedAt")))
        if best is None or key > best[0]:
            best = (key, d.get("score"), date_)
    return best


def reconcile(store: Store, ratings_lookup, stale_days: int = 3, now: datetime | None = None) -> ReconcileSummary:
    """Record the new score once Mongo (synced from FactorView) shows a report newer than the one we sent."""
    now = now or datetime.now(timezone.utc)
    summary = ReconcileSummary()
    rows = store.unresolved_accepted()
    ratings = ratings_lookup({r["debtor_id"] for r in rows}) if rows else {}
    for row in rows:
        summary.checked += 1
        best = _newest_rating(ratings.get(row["debtor_id"], []))
        if best and best[2] > row["report_date"]:
            score = best[1] if best[1] is None or isinstance(best[1], str) else str(best[1])
            store.mark_resolved(row["event_key"], score, best[2])
            summary.resolved += 1
            log.info("resolved event_id=%s debtor_id=%s new_report_date=%s", row["event_id"], row["debtor_id"], best[2])
            continue
        summary.unresolved += 1
        accepted = datetime.fromisoformat(row["accepted_at"].replace("Z", "+00:00"))
        if (now - accepted).days >= stale_days:
            summary.stale += 1
            log.warning("event_id=%s debtor_id=%s accepted %d+ days ago, no newer credit report in Mongo yet",
                        row["event_id"], row["debtor_id"], stale_days)
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
