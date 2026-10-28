"""SQLite delivery tracking. All timestamps are UTC ISO-8601."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .events import utc_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_key TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    debtor_id TEXT NOT NULL,
    report_date TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','failed','accepted')),
    attempts INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL,
    last_attempt_at TEXT,
    accepted_at TEXT,
    last_http_status INTEGER,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    command TEXT NOT NULL,
    options_json TEXT,
    summary_json TEXT,
    aborted TEXT
);
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    event_id TEXT NOT NULL,
    debtor_id TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    http_status INTEGER,
    error TEXT,
    tries INTEGER
);
CREATE TABLE IF NOT EXISTS score_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    debtor_id TEXT NOT NULL,
    report_date TEXT NOT NULL,
    score TEXT,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    event_id TEXT,
    UNIQUE (debtor_id, report_date, source)
);
"""


NEW_COLUMNS = {"resolved_at": "TEXT", "new_score": "TEXT", "new_report_date": "TEXT"}


class Store:
    def __init__(self, path: Path | None, readonly: bool = False):
        """path=None or a missing DB in readonly mode -> empty in-memory view."""
        self.persistent = True
        self.run_id: int | None = None
        if path is not None and readonly and not path.exists():
            path = None
        if path is None:
            self.conn = sqlite3.connect(":memory:")
            self.persistent = False
        elif readonly:
            self.conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        if not readonly or not self.persistent:
            self.conn.executescript(SCHEMA)
            have = {r[1] for r in self.conn.execute("PRAGMA table_info(events)")}
            for col, typ in NEW_COLUMNS.items():
                if col not in have:
                    self.conn.execute(f"ALTER TABLE events ADD COLUMN {col} {typ}")
            self._backfill_scores()
            self.conn.commit()

    def close(self):
        self.conn.close()

    def get(self, event_key: str):
        return self.conn.execute("SELECT * FROM events WHERE event_key=?", (event_key,)).fetchone()

    def get_by_event_id(self, event_id: str):
        return self.conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()

    def ensure(self, event_key: str, payload: dict):
        """Insert as pending if unseen. Existing rows (and their stored payload) are kept."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO events (event_key,event_id,debtor_id,report_date,payload_json,status,first_seen_at)"
            " VALUES (?,?,?,?,?,'pending',?)",
            (event_key, payload["event_id"], payload["debtor_id"], payload["credit_report_date"],
             json.dumps(payload, sort_keys=True), utc_iso()))
        if cur.rowcount:
            self._add_score(payload["debtor_id"], payload["credit_report_date"], payload.get("credit_score"),
                            "detected", payload["event_id"])
        self.conn.commit()
        return self.get(event_key)

    def refresh_payload(self, event_key: str, payload: dict):
        """Update an unaccepted event's payload (e.g. new client/rep data); event_id and detected_at are kept."""
        row = self.get(event_key)
        if row is None or row["status"] == "accepted":
            return row
        old = json.loads(row["payload_json"])
        payload = dict(payload, event_id=old["event_id"], detected_at=old["detected_at"])
        self.conn.execute("UPDATE events SET payload_json=? WHERE event_key=?",
                          (json.dumps(payload, sort_keys=True), event_key))
        self.conn.commit()
        return self.get(event_key)

    def _backfill_scores(self):
        """Seed score_history from events recorded before the history tables existed (idempotent)."""
        for r in self.conn.execute("SELECT * FROM events").fetchall():
            self._add_score(r["debtor_id"], r["report_date"], json.loads(r["payload_json"]).get("credit_score"),
                            "detected", r["event_id"])
            if r["resolved_at"]:
                self._add_score(r["debtor_id"], r["new_report_date"], r["new_score"], "refreshed", r["event_id"])

    def _add_score(self, debtor_id, report_date, score, source, event_id):
        self.conn.execute(
            "INSERT OR IGNORE INTO score_history (debtor_id,report_date,score,observed_at,source,event_id)"
            " VALUES (?,?,?,?,?,?)", (debtor_id, report_date, score, utc_iso(), source, event_id))

    def start_run(self, command: str, options: dict) -> int:
        cur = self.conn.execute("INSERT INTO runs (started_at, command, options_json) VALUES (?,?,?)",
                                (utc_iso(), command, json.dumps(options, sort_keys=True)))
        self.conn.commit()
        self.run_id = cur.lastrowid
        return self.run_id

    def finish_run(self, summary: dict, aborted: str | None = None):
        if self.run_id is None:
            return
        self.conn.execute("UPDATE runs SET finished_at=?, summary_json=?, aborted=? WHERE run_id=?",
                          (utc_iso(), json.dumps(summary, sort_keys=True), aborted, self.run_id))
        self.conn.commit()

    def runs(self, limit: int = 20):
        return self.conn.execute("SELECT * FROM runs ORDER BY run_id DESC LIMIT ?", (limit,)).fetchall()

    def debtor_history(self, debtor_id: str):
        q = self.conn.execute
        return {
            "events": q("SELECT * FROM events WHERE debtor_id=? ORDER BY first_seen_at", (debtor_id,)).fetchall(),
            "scores": q("SELECT * FROM score_history WHERE debtor_id=? ORDER BY observed_at", (debtor_id,)).fetchall(),
            "attempts": q("SELECT * FROM attempts WHERE debtor_id=? ORDER BY id", (debtor_id,)).fetchall(),
        }

    def backup_to(self, out_path: Path):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        dest = sqlite3.connect(out_path)
        try:
            self.conn.backup(dest)
        finally:
            dest.close()

    def record_attempt(self, event_key: str, accepted: bool, http_status: int | None,
                       error: str | None, attempts: int):
        now = utc_iso()
        ev = self.get(event_key)
        if ev is not None:
            self.conn.execute(
                "INSERT INTO attempts (run_id,event_id,debtor_id,attempted_at,outcome,http_status,error,tries)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (self.run_id, ev["event_id"], ev["debtor_id"], now, "accepted" if accepted else "failed",
                 http_status, error, attempts))
        if accepted:
            self.conn.execute(
                "UPDATE events SET status='accepted', attempts=attempts+?, last_attempt_at=?,"
                " accepted_at=?, last_http_status=?, last_error=NULL WHERE event_key=?",
                (attempts, now, now, http_status, event_key))
        else:
            # Never downgrade an already accepted event (e.g. a failed replay).
            self.conn.execute(
                "UPDATE events SET status=CASE WHEN status='accepted' THEN status ELSE 'failed' END,"
                " attempts=attempts+?, last_attempt_at=?, last_http_status=?, last_error=? WHERE event_key=?",
                (attempts, now, http_status, error, event_key))
        self.conn.commit()

    def unresolved_accepted(self):
        """Accepted active-route events whose new score has not been observed yet."""
        have = {r[1] for r in self.conn.execute("PRAGMA table_info(events)")}
        cond = " AND resolved_at IS NULL" if "resolved_at" in have else ""  # old DB: nothing resolved yet
        rows = self.conn.execute(
            f"SELECT * FROM events WHERE status='accepted'{cond} ORDER BY accepted_at").fetchall()
        return [r for r in rows if json.loads(r["payload_json"]).get("is_active") is not False]

    def overwrite_payload(self, event_key: str, payload: dict):
        """Replace a stored payload (also for accepted events), e.g. for an explicit refreshed replay."""
        self.conn.execute("UPDATE events SET payload_json=? WHERE event_key=?",
                          (json.dumps(payload, sort_keys=True), event_key))
        self.conn.commit()

    def latest_event_for_debtor(self, debtor_id: str):
        return self.conn.execute("SELECT * FROM events WHERE debtor_id=? ORDER BY first_seen_at DESC LIMIT 1",
                                 (debtor_id,)).fetchone()

    def mark_resolved(self, event_key: str, new_score, new_report_date: str):
        self.conn.execute("UPDATE events SET resolved_at=?, new_score=?, new_report_date=? WHERE event_key=?",
                          (utc_iso(), new_score, new_report_date, event_key))
        ev = self.get(event_key)
        self._add_score(ev["debtor_id"], new_report_date, new_score, "refreshed", ev["event_id"])
        self.conn.commit()

    def list(self, status: str | None = None, limit: int = 50):
        sql, args = "SELECT * FROM events", []
        if status:
            sql += " WHERE status=?"
            args.append(status)
        sql += " ORDER BY COALESCE(last_attempt_at, first_seen_at) DESC LIMIT ?"
        args.append(limit)
        return self.conn.execute(sql, args).fetchall()

    def counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT status, COUNT(*) c FROM events GROUP BY status").fetchall()
        return {r["status"]: r["c"] for r in rows}
