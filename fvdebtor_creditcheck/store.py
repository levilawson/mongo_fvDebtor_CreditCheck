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
"""


NEW_COLUMNS = {"resolved_at": "TEXT", "new_score": "TEXT", "new_report_date": "TEXT"}


class Store:
    def __init__(self, path: Path | None, readonly: bool = False):
        """path=None or a missing DB in readonly mode -> empty in-memory view."""
        self.persistent = True
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
            self.conn.commit()

    def close(self):
        self.conn.close()

    def get(self, event_key: str):
        return self.conn.execute("SELECT * FROM events WHERE event_key=?", (event_key,)).fetchone()

    def get_by_event_id(self, event_id: str):
        return self.conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()

    def ensure(self, event_key: str, payload: dict):
        """Insert as pending if unseen. Existing rows (and their stored payload) are kept."""
        self.conn.execute(
            "INSERT OR IGNORE INTO events (event_key,event_id,debtor_id,report_date,payload_json,status,first_seen_at)"
            " VALUES (?,?,?,?,?,'pending',?)",
            (event_key, payload["event_id"], payload["debtor_id"], payload["credit_report_date"],
             json.dumps(payload, sort_keys=True), utc_iso()))
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

    def record_attempt(self, event_key: str, accepted: bool, http_status: int | None,
                       error: str | None, attempts: int):
        now = utc_iso()
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
        rows = self.conn.execute(
            "SELECT * FROM events WHERE status='accepted' AND resolved_at IS NULL ORDER BY accepted_at").fetchall()
        return [r for r in rows if json.loads(r["payload_json"]).get("is_active") is not False]

    def mark_resolved(self, event_key: str, new_score, new_report_date: str):
        self.conn.execute("UPDATE events SET resolved_at=?, new_score=?, new_report_date=? WHERE event_key=?",
                          (utc_iso(), new_score, new_report_date, event_key))
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
