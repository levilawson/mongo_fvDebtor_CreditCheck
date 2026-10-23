"""Validation, deduplication and payload construction (pure functions)."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

SCHEMA_VERSION = 1
EVENT_TYPE = "factorview.credit_report_expired"
EVENT_TYPE_INACTIVE = "factorview.credit_report_expired_inactive"
INACTIVE_SUFFIX = "|inactive"

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class InvalidRecord(Exception):
    """Raised with a short, secret-free reason."""


def utc_iso(dt: datetime | None = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_debtor_id(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise InvalidRecord("missing or malformed debtor id")
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str) or not _ID_RE.match(value.strip()):
        raise InvalidRecord("missing or malformed debtor id")
    return value.strip()


def parse_report_date(value: Any) -> str:
    """Strict YYYY-MM-DD. No default is ever substituted."""
    if not isinstance(value, str) or not _DATE_RE.match(value.strip()):
        raise InvalidRecord("missing or malformed credit report date")
    try:
        date.fromisoformat(value.strip())
    except ValueError:
        raise InvalidRecord("invalid credit report date") from None
    return value.strip()


# Clients never included in events: FactorView's commented-out convention ('Z - ...') and test clients
# whose name starts with the word "Test" ("Test Company", "Test - ABC"; not "Testa Energy" or "Contest Co").
_EXCLUDED_CLIENT_RES = [re.compile(r"^\s*z\s*-", re.IGNORECASE), re.compile(r"^\s*test\b", re.IGNORECASE)]


def is_excluded_client(name: Any, extra: "re.Pattern | None" = None) -> bool:
    if not isinstance(name, str):
        return False
    return any(r.match(name) for r in _EXCLUDED_CLIENT_RES + ([extra] if extra else []))


def _join_and(items: list[str]) -> str:
    if len(items) <= 2:
        return " and ".join(items)
    return ", ".join(items[:-1]) + ", and " + items[-1]


def client_label(client_id: str, client_name: str | None) -> str:
    return f"{client_name or 'Unnamed client'} (FV ID: {client_id})"


def build_rep_groups(clients: list[dict]) -> list[dict]:
    """One entry per account rep (null rep last) with a ready-to-use client sentence fragment."""
    groups: dict[str | None, list[dict]] = {}
    for c in clients:
        groups.setdefault(c["account_rep"], []).append(
            {"client_id": c["client_id"], "client_name": c["client_name"], "client_label": c["client_label"]})
    out = []
    for rep in sorted(groups, key=lambda r: (r is None, (r or "").lower())):
        members = groups[rep]
        labels = [m["client_label"] for m in members]
        out.append({"account_rep": rep, "client_count": len(members), "clients": members,
                    "clients_text": _join_and(labels), "clients_lines": "\n".join(labels)})
    return out


def parse_client_id(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        value = str(value)
    if isinstance(value, str) and _ID_RE.match(value.strip()):
        return value.strip()
    return None


def make_event_key(debtor_id: str, report_date: str) -> str:
    return f"{debtor_id}|{report_date}"


def make_event_id(debtor_id: str, report_date: str, active: bool = True) -> str:
    key = make_event_key(debtor_id, report_date) + ("" if active else INACTIVE_SUFFIX)
    digest = hashlib.sha256(key.encode()).hexdigest()
    return f"fvcr-{digest[:32]}"


def _updated_sort_key(value: Any) -> tuple[int, float]:
    """Newest-wins ordering; unparsable/missing values sort oldest."""
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return (1, dt.timestamp())
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return (1, dt.timestamp())
        except ValueError:
            pass
    return (0, 0.0)


def _serialize_updated_at(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return utc_iso(value)
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


@dataclass
class Candidate:
    event_key: str
    debtor_id: str
    report_date: str
    doc: dict
    updated_sort: tuple[int, float]
    client_ids: set = field(default_factory=set)
    is_active: bool = True

    def store_key(self) -> str:
        """Active events keep the plain key; inactive-route events are tracked separately."""
        return self.event_key + ("" if self.is_active else INACTIVE_SUFFIX)

    def build_payload(self, detected_at: str, companies: dict | None = None, window_days: int = 90,
                      validity_days: int = 30) -> dict:
        companies = companies or {}
        clients = []
        for cid in sorted(self.client_ids):
            co = companies.get(cid) or {}
            clients.append({"client_id": cid, "client_name": co.get("name"),
                            "client_label": client_label(cid, co.get("name")),
                            "account_rep": co.get("accountRep")})
        d = self.doc.get("doc") or {}
        cr = d.get("creditRating") or {}
        score = cr.get("score")
        avg = cr.get("averageDays")
        name = d.get("name")
        payload = {
            "schema_version": SCHEMA_VERSION,
            "event_type": EVENT_TYPE if self.is_active else EVENT_TYPE_INACTIVE,
            "event_id": make_event_id(self.debtor_id, self.report_date, self.is_active),
            "detected_at": detected_at,
            "debtor_id": self.debtor_id,
            "debtor_name": name if isinstance(name, str) else None,
            "credit_score": score if isinstance(score, str) else (None if score is None else str(score)),
            "credit_report_date": self.report_date,
            "credit_report_expires_on": (date.fromisoformat(self.report_date) + timedelta(days=validity_days)).isoformat(),
            "credit_average_days": avg if isinstance(avg, int) and not isinstance(avg, bool) else None,
            "credit_report_expired": True,
            "is_active": self.is_active,
            "restricted": d.get("restricted") if isinstance(d.get("restricted"), bool) else None,
            "mongo_updated_at": _serialize_updated_at(self.doc.get("updatedAt")),
            "client_count": len(clients),
            "clients": clients,
            "rep_groups": build_rep_groups(clients),
        }
        if not self.is_active:
            payload["inactive_reason"] = f"no invoices purchased in {window_days} days or more"
            payload["inactivity_window_days"] = window_days
        return payload


@dataclass
class Collected:
    candidates: list[Candidate]
    matched: int
    duplicate: int
    invalid: list[str]  # secret-free descriptions for the log


def collect_candidates(docs: Iterable[dict]) -> Collected:
    best: dict[str, Candidate] = {}
    matched = duplicate = 0
    invalid: list[str] = []
    for raw in docs:
        matched += 1
        doc = (raw or {}).get("doc") or {}
        try:
            debtor_id = parse_debtor_id(doc.get("id"))
            report_date = parse_report_date((doc.get("creditRating") or {}).get("date"))
        except InvalidRecord as exc:
            # Mongo _id is not projected; identify by whatever id text is safe to show.
            raw_id = doc.get("id")
            label = raw_id if isinstance(raw_id, (str, int)) and len(str(raw_id)) <= 64 else "?"
            invalid.append(f"debtor_id={label!r}: {exc}")
            continue
        cand = Candidate(make_event_key(debtor_id, report_date), debtor_id, report_date,
                         raw, _updated_sort_key(raw.get("updatedAt")))
        client_id = parse_client_id(raw.get("clientId"))
        existing = best.get(cand.event_key)
        if existing is None:
            best[cand.event_key] = cand
        else:
            duplicate += 1
            cand.client_ids |= existing.client_ids
            if cand.updated_sort > existing.updated_sort:
                best[cand.event_key] = cand
            else:
                cand = existing
        if client_id:
            cand.client_ids.add(client_id)
    ordered = sorted(best.values(), key=lambda c: (c.debtor_id, c.report_date))
    return Collected(ordered, matched, duplicate, invalid)
