"""Read-only Mongo access."""
from __future__ import annotations

from typing import Iterator

from pymongo import MongoClient

from .config import MONGO_COLLECTION, MONGO_DB

QUERY = {"doc.creditReportExpired": True}
PROJECTION = {
    "_id": 0,
    "doc.id": 1,
    "doc.name": 1,
    "doc.creditRating.score": 1,
    "doc.creditRating.date": 1,
    "doc.creditRating.averageDays": 1,
    "doc.creditReportExpired": 1,
    "doc.restricted": 1,
    "updatedAt": 1,
    "clientId": 1,
}
COMPANY_PROJECTION = {"_id": 0, "doc.id": 1, "doc.name": 1, "doc.accountRep": 1}


class MongoReadError(Exception):
    """Wraps driver errors without carrying their (possibly sensitive) text."""


def _open(uri, client_factory):
    return client_factory(uri, serverSelectionTimeoutMS=15000, connectTimeoutMS=10000,
                          socketTimeoutMS=60000, appname="fvdebtor-creditcheck")


def fetch_companies(uri: str, ids, client_factory=MongoClient) -> dict[str, dict]:
    """fvCompany records keyed by doc.id (client IDs resolve here). Read-only."""
    ids = sorted({str(i) for i in ids})
    out: dict[str, dict] = {}
    if not ids:
        return out
    client = None
    try:
        client = _open(uri, client_factory)
        coll = client[MONGO_DB]["fvCompany"]
        for i in range(0, len(ids), 500):
            for d in coll.find({"doc.id": {"$in": ids[i:i + 500]}}, COMPANY_PROJECTION):
                x = d.get("doc") or {}
                out[str(x.get("id"))] = {"name": x.get("name"), "accountRep": x.get("accountRep")}
        return out
    except Exception as exc:  # noqa: BLE001
        raise MongoReadError(type(exc).__name__) from None
    finally:
        if client is not None:
            client.close()


def fetch_debtor_clients(uri: str, debtor_ids, client_factory=MongoClient) -> dict[str, set]:
    """Every clientId linked to each debtor, regardless of the expired flag. Read-only."""
    ids = sorted({str(i) for i in debtor_ids})
    out: dict[str, set] = {}
    if not ids:
        return out
    client = None
    try:
        client = _open(uri, client_factory)
        coll = client[MONGO_DB][MONGO_COLLECTION]
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            chunk = chunk + [int(x) for x in chunk if x.isdigit()]
            for d in coll.find({"doc.id": {"$in": chunk}}, {"_id": 0, "doc.id": 1, "clientId": 1}):
                cid = d.get("clientId")
                if cid is not None:
                    out.setdefault(str((d.get("doc") or {}).get("id")), set()).add(str(cid))
        return out
    except Exception as exc:  # noqa: BLE001
        raise MongoReadError(type(exc).__name__) from None
    finally:
        if client is not None:
            client.close()


class SuspiciousActivityData(Exception):
    """No purchases at all in the window: the date field/format is probably not what we expect."""


def fetch_active_debtors(uri: str, since_date: str, client_factory=MongoClient) -> set[str]:
    """Debtor IDs with at least one purchased invoice (isPurchase, buyDate >= since_date). Read-only.

    buyDate is compared as an ISO string (YYYY-MM-DD...). Uses the (isPurchase, buyDate) index.
    """
    client = None
    try:
        client = _open(uri, client_factory)
        coll = client[MONGO_DB]["fvInvoice"]
        ids = coll.distinct("doc.debtorId", {"doc.isPurchase": True, "doc.buyDate": {"$gte": since_date}})
    except Exception as exc:  # noqa: BLE001
        raise MongoReadError(type(exc).__name__) from None
    finally:
        if client is not None:
            client.close()
    active = {str(i) for i in ids if i is not None}
    if not active:
        raise SuspiciousActivityData("no purchased invoices found in the activity window")
    return active


def fetch_current_ratings(uri: str, debtor_ids, client_factory=MongoClient) -> dict[str, list[dict]]:
    """Current creditRating (score/date) and updatedAt of every fvDebtor document per debtor. Read-only."""
    ids = sorted({str(i) for i in debtor_ids})
    out: dict[str, list[dict]] = {}
    if not ids:
        return out
    client = None
    try:
        client = _open(uri, client_factory)
        coll = client[MONGO_DB][MONGO_COLLECTION]
        proj = {"_id": 0, "doc.id": 1, "doc.creditRating.score": 1, "doc.creditRating.date": 1, "updatedAt": 1}
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            chunk = chunk + [int(x) for x in chunk if x.isdigit()]
            for d in coll.find({"doc.id": {"$in": chunk}}, proj):
                x = d.get("doc") or {}
                out.setdefault(str(x.get("id")), []).append(
                    {"score": (x.get("creditRating") or {}).get("score"),
                     "date": (x.get("creditRating") or {}).get("date"), "updatedAt": d.get("updatedAt")})
        return out
    except Exception as exc:  # noqa: BLE001
        raise MongoReadError(type(exc).__name__) from None
    finally:
        if client is not None:
            client.close()


def fetch_expired(uri: str, client_factory=MongoClient) -> list[dict]:
    client = None
    try:
        client = _open(uri, client_factory)
        coll = client[MONGO_DB][MONGO_COLLECTION]
        return list(coll.find(QUERY, PROJECTION))
    except Exception as exc:  # noqa: BLE001 - never surface driver text (may include URI/host)
        raise MongoReadError(type(exc).__name__) from None
    finally:
        if client is not None:
            client.close()
