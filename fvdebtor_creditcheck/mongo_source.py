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
