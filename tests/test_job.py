import json
from datetime import datetime, timezone

import pytest
import requests

from fvdebtor_creditcheck import mongo_source, runner, webhook
from fvdebtor_creditcheck.events import collect_candidates, make_event_id
from fvdebtor_creditcheck.store import Store


def doc(i="1888", date="2026-09-16", upd="2026-10-01T00:00:00Z", score="70/B", client="2565"):
    return {"clientId": client, "doc": {"id": i, "name": f"Debtor {i}", "restricted": False,
                    "creditRating": {"score": score, "date": date, "averageDays": 0},
                    "creditReportExpired": True}, "updatedAt": upd}


class Resp:
    def __init__(self, status, headers=None):
        self.status_code, self.headers = status, headers or {}


class FakeSession:
    def __init__(self, *outcomes):
        self.outcomes, self.calls = list(outcomes), []

    def post(self, url, **kw):
        self.calls.append((url, kw))
        o = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(o, Exception):
            raise o
        return o


def client(session, sleeps=None, attempts=4):
    return webhook.WebhookClient("https://hooks.example/x", 1, 2, attempts, 2, 60,
                                 session=session, sleep=(sleeps.append if sleeps is not None else lambda s: None))


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "s.sqlite3")
    yield s
    s.close()


# ---- Mongo ----
def test_mongo_query_projection_and_error_redaction():
    seen = {}

    class Coll:
        def find(self, q, p):
            seen["q"], seen["p"] = q, p
            return [doc()]

    class Cl:
        def __init__(self, uri, **kw): pass
        def __getitem__(self, n): return {"fvDebtor": Coll()} if False else self
        def close(self): pass

    class Db:
        def __getitem__(self, n):
            assert n == "fvDebtor"
            return Coll()

    class Cl2(Cl):
        def __getitem__(self, n):
            assert n == "payments-service"
            return Db()

    assert mongo_source.fetch_expired("mongodb://u:pw@h", Cl2) == [doc()]
    assert seen["q"] == {"doc.creditReportExpired": True}
    assert "doc.creditRating.averageDays" in seen["p"] and "doc.restricted" in seen["p"]

    def boom(uri, **kw):
        raise RuntimeError("bad auth for mongodb://u:SECRETPW@h")

    with pytest.raises(mongo_source.MongoReadError) as e:
        mongo_source.fetch_expired("x", boom)
    assert "SECRETPW" not in str(e.value) and e.value.__cause__ is None


# ---- validation / dedupe ----
def test_validation_never_substitutes_date():
    docs = [doc(date="2026-13-45"), doc(date=None), doc(date="20260916"), doc(i=None), doc(i="a b"), doc()]
    c = collect_candidates(docs)
    assert len(c.invalid) == 5 and len(c.candidates) == 1 and c.matched == 6


def test_duplicate_records_newest_updated_at_wins():
    older = doc(upd="2026-10-01T00:00:00Z", score="60/C")
    newer = doc(upd=datetime(2026, 10, 5, tzinfo=timezone.utc), score="70/B")
    c = collect_candidates([older, newer, doc(upd="2026-09-01T00:00:00Z", score="50/D")])
    assert c.duplicate == 2 and len(c.candidates) == 1
    p = c.candidates[0].build_payload("2026-10-21T00:00:00Z")
    assert p["credit_score"] == "70/B" and p["mongo_updated_at"] == "2026-10-05T00:00:00Z"


def test_payload_shape_and_string_preservation():
    p = collect_candidates([doc(i=1888)]).candidates[0].build_payload("t")
    assert p["debtor_id"] == "1888" and isinstance(p["credit_score"], str)
    assert p["event_id"] == make_event_id("1888", "2026-09-16")
    assert set(p) == {"schema_version", "event_type", "event_id", "detected_at", "debtor_id", "debtor_name",
                      "credit_score", "credit_report_date", "credit_average_days", "credit_report_expired",
                      "restricted", "mongo_updated_at", "client_count", "clients", "rep_groups"}


# ---- dry run / send / dedup / limit ----
def test_dry_run_sends_nothing_and_writes_nothing(store):
    sess = FakeSession(Resp(200))
    s = runner.run_job([doc("1"), doc("2")], store, client(sess), send=False, limit=None)
    assert s.eligible == 2 and s.accepted == 0 and not sess.calls
    assert store.counts() == {}


def test_send_then_rerun_does_not_resend(store):
    sess = FakeSession(Resp(200))
    docs = [doc("1"), doc("2")]
    assert runner.run_job(docs, store, client(sess), True, None).accepted == 2
    s2 = runner.run_job(docs, store, client(sess), True, None)
    assert s2.already_sent == 2 and s2.accepted == 0 and len(sess.calls) == 2


def test_new_report_date_is_new_event(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1", "2026-09-16")], store, client(sess), True, None)
    s = runner.run_job([doc("1", "2026-10-10")], store, client(sess), True, None)
    assert s.accepted == 1 and len(sess.calls) == 2
    ids = {json.loads(c[1]["json"] if isinstance(c[1]["json"], str) else json.dumps(c[1]["json"]))["event_id"]
           for c in sess.calls}
    assert len(ids) == 2


def test_limit_applies_to_unsent_events_only(store):
    sess = FakeSession(Resp(200))
    docs = [doc(str(i)) for i in range(1, 5)]
    runner.run_job(docs[:2], store, client(sess), True, None)  # 1,2 already sent
    s = runner.run_job(docs, store, client(sess), True, 1)
    assert s.already_sent == 2 and s.eligible == 2 and s.selected == 1 and s.accepted == 1
    assert sess.calls[-1][1]["json"]["debtor_id"] == "3"


def test_one_post_per_debtor_with_tls_and_timeout(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1"), doc("2")], store, client(sess), True, None)
    assert len(sess.calls) == 2
    for _, kw in sess.calls:
        assert kw["verify"] is True and kw["timeout"] == (1, 2) and kw["allow_redirects"] is False


# ---- failures / retries ----
def test_failed_event_retained_and_retried_next_run_with_same_id(store):
    docs = [doc("1")]
    runner.run_job(docs, store, client(FakeSession(Resp(400))), True, None)
    row = store.get("1|2026-09-16")
    assert row["status"] == "failed" and row["accepted_at"] is None
    first_id = row["event_id"]
    s = runner.run_job(docs, store, client(FakeSession(Resp(200))), True, None)
    row = store.get("1|2026-09-16")
    assert s.accepted == 1 and row["status"] == "accepted" and row["event_id"] == first_id


def test_retry_on_5xx_and_429_then_success_respects_retry_after():
    sleeps = []
    sess = FakeSession(Resp(503), Resp(429, {"Retry-After": "7"}), Resp(200))
    r = client(sess, sleeps).send({"a": 1})
    assert r.outcome == webhook.ACCEPTED and r.attempts == 3 and sleeps == [2, 7]


def test_retry_after_is_capped_and_attempts_bounded():
    sleeps = []
    sess = FakeSession(Resp(429, {"Retry-After": "99999"}))
    r = client(sess, sleeps, attempts=3).send({})
    assert r.outcome == webhook.FAILED and len(sess.calls) == 3 and sleeps == [60, 60]


def test_connection_error_and_timeout_retried_without_leaking_url():
    sess = FakeSession(requests.ConnectionError("https://hooks.example/SECRET"), requests.Timeout(), Resp(200))
    assert client(sess).send({}).outcome == webhook.ACCEPTED
    sess = FakeSession(requests.Timeout())
    r = client(sess, attempts=2).send({})
    assert r.outcome == webhook.FAILED and "unknown" in r.error
    sess = FakeSession(requests.ConnectionError("https://hooks.example/SECRET"))
    assert "SECRET" not in client(sess, attempts=1).send({}).error


def test_permanent_4xx_not_retried():
    sess = FakeSession(Resp(400))
    r = client(sess).send({})
    assert r.outcome == webhook.FAILED and len(sess.calls) == 1


def test_404_stops_run_and_keeps_remaining_events(store):
    sess = FakeSession(Resp(404))
    s = runner.run_job([doc("1"), doc("2"), doc("3")], store, client(sess), True, None)
    assert len(sess.calls) == 1 and s.aborted and s.failed == 1
    assert store.get("2|2026-09-16") is None  # untouched, will be picked up later


def test_consecutive_failures_abort(store):
    sess = FakeSession(Resp(400))
    s = runner.run_job([doc(str(i)) for i in range(1, 6)], store, client(sess), True, None, 2)
    assert s.failed == 2 and s.aborted


# ---- replay / lock ----
def test_replay_keeps_event_id_and_payload(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1")], store, client(sess), True, None)
    original = sess.calls[0][1]["json"]
    eid = original["event_id"]
    runner.replay_event(store, client(sess), eid)
    assert sess.calls[1][1]["json"] == original
    with pytest.raises(KeyError):
        runner.replay_event(store, client(sess), "nope")


def test_failed_replay_does_not_unaccept(store):
    runner.run_job([doc("1")], store, client(FakeSession(Resp(200))), True, None)
    eid = store.get("1|2026-09-16")["event_id"]
    runner.replay_event(store, client(FakeSession(Resp(400))), eid)
    assert store.get("1|2026-09-16")["status"] == "accepted"


def test_overlapping_runs_blocked(tmp_path):
    lock = tmp_path / "l.lock"
    with runner.single_instance(lock):
        with pytest.raises(runner.AlreadyRunning):
            with runner.single_instance(lock):
                pass
    with runner.single_instance(lock):
        pass


def test_debtor_id_filter_sends_only_that_debtor(store):
    sess = FakeSession(Resp(200))
    s = runner.run_job([doc("1"), doc("2303"), doc("3")], store, client(sess), True, None, debtor_id="2303")
    assert s.eligible == 1 and s.accepted == 1
    assert [c[1]["json"]["debtor_id"] for c in sess.calls] == ["2303"]


COMPANIES = {"2565": {"name": "Raine Energy Services LLC", "accountRep": "Christine Dunn"},
             "1479": {"name": "Other Client", "accountRep": None}}


def test_clients_unioned_across_duplicates_and_enriched(store):
    sess = FakeSession(Resp(200))
    docs = [doc("1892", client="2565", upd="2026-10-01T00:00:00Z"),
            doc("1892", client="1479", upd="2026-10-05T00:00:00Z"),
            doc("1892", client="9999")]
    s = runner.run_job(docs, store, client(sess), True, None, company_lookup=lambda ids: COMPANIES)
    assert s.duplicate == 2 and s.clients == 3 and s.clients_unresolved == 1 and len(sess.calls) == 1
    assert sess.calls[0][1]["json"]["clients"] == [
        {"client_id": "1479", "client_name": "Other Client", "account_rep": None},
        {"client_id": "2565", "client_name": "Raine Energy Services LLC", "account_rep": "Christine Dunn"},
        {"client_id": "9999", "client_name": None, "account_rep": None}]


def test_failed_event_payload_refreshed_but_event_id_and_detected_at_kept(store):
    runner.run_job([doc("1")], store, client(FakeSession(Resp(400))), True, None)
    first = json.loads(store.get("1|2026-09-16")["payload_json"])
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1")], store, client(sess), True, None, company_lookup=lambda ids: COMPANIES)
    sent = sess.calls[0][1]["json"]
    assert sent["event_id"] == first["event_id"] and sent["detected_at"] == first["detected_at"]
    assert sent["clients"][0]["account_rep"] == "Christine Dunn"


def test_company_lookup_queries_fvcompany_in_one_batch():
    seen = {}

    class Coll:
        def find(self, q, p):
            seen["q"] = q
            return [{"doc": {"id": "2565", "name": "Raine", "accountRep": "Christine Dunn"}}]

    class Db:
        def __getitem__(self, n):
            seen["coll"] = n
            return Coll()

    class Cl:
        def __init__(self, uri, **kw): pass
        def __getitem__(self, n): return Db()
        def close(self): pass

    out = mongo_source.fetch_companies("u", ["2565", 2565, "1"], Cl)
    assert seen["coll"] == "fvCompany" and seen["q"] == {"doc.id": {"$in": ["1", "2565"]}}
    assert out["2565"]["accountRep"] == "Christine Dunn"


def test_cli_dry_run_end_to_end(tmp_path, monkeypatch, capsys):
    from fvdebtor_creditcheck import cli
    monkeypatch.setenv("MONGO_URI", "mongodb://u:pw@h")
    monkeypatch.setenv("ZAPIER_WEBHOOK_URL", "https://hooks.example/x")
    monkeypatch.setenv("STATE_DB_PATH", str(tmp_path / "s.sqlite3"))
    monkeypatch.setenv("LOCK_PATH", str(tmp_path / "l.lock"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(cli, "fetch_expired", lambda uri: [doc("1892", client="2565"), doc("5")])
    monkeypatch.setattr(cli, "fetch_companies", lambda uri, ids: COMPANIES)
    monkeypatch.setattr(cli, "fetch_debtor_clients", lambda uri, ids: {"1892": {"2565", "1479"}})
    assert cli.main(["run", "--debtor-id", "1892"]) == 0
    out = capsys.readouterr().out
    assert "['1892']" in out and "clients=2" in out and "mode=dry-run" in out
    assert not (tmp_path / "s.sqlite3").exists()
    assert cli.main(["run", "--debtor-id", "1892", "--show-payload"]) == 0
    out = capsys.readouterr().out
    assert '"rep_groups"' in out and '"account_rep": "Christine Dunn"' in out and "hooks.example" not in out
    assert cli.main(["run", "--debtor-id", "1892-show-payload"]) == 0
    assert "no eligible event" in capsys.readouterr().out


def test_all_linked_clients_included_even_if_their_record_not_expired(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1892", client="2565")], store, client(sess), True, None,
                   company_lookup=lambda ids: COMPANIES,
                   debtor_clients_lookup=lambda ids: {"1892": {"2565", "1479"}})
    assert [c["client_id"] for c in sess.calls[0][1]["json"]["clients"]] == ["1479", "2565"]


def test_debtor_clients_query_ignores_expired_flag():
    seen = {}

    class Coll:
        def find(self, q, p):
            seen["q"] = q
            return [{"doc": {"id": "1892"}, "clientId": "2565"}, {"doc": {"id": 1892}, "clientId": "1479"}]

    class Db:
        def __getitem__(self, n): return Coll()

    class Cl:
        def __init__(self, uri, **kw): pass
        def __getitem__(self, n): return Db()
        def close(self): pass

    out = mongo_source.fetch_debtor_clients("u", ["1892"], Cl)
    assert out == {"1892": {"2565", "1479"}} and "doc.creditReportExpired" not in seen["q"]


def test_rep_groups_are_per_rep_with_message_text():
    from fvdebtor_creditcheck.events import build_rep_groups
    clients = [
        {"client_id": "2565", "client_name": "Raine Energy LLC", "account_rep": "AE A"},
        {"client_id": "1", "client_name": "Client ABC", "account_rep": "AE A"},
        {"client_id": "2", "client_name": "Client BCD", "account_rep": "AE A"},
        {"client_id": "3", "client_name": "Client CDE", "account_rep": "AE B"},
        {"client_id": "4", "client_name": None, "account_rep": None},
    ]
    g = build_rep_groups(clients)
    assert [x["account_rep"] for x in g] == ["AE A", "AE B", None]
    assert g[0]["clients_text"] == "Raine Energy LLC (FV ID: 2565), Client ABC (FV ID: 1), and Client BCD (FV ID: 2)"
    assert g[1]["clients_text"] == "Client CDE (FV ID: 3)" and g[1]["client_count"] == 1
    assert g[2]["clients_text"] == "Unnamed client (FV ID: 4)"


def test_z_prefixed_clients_excluded(store):
    sess = FakeSession(Resp(200))
    companies = {"1": {"name": "Good Co", "accountRep": "AE A"},
                 "2": {"name": "Z - Old Co", "accountRep": None},
                 "3": {"name": " z-Old Co 2", "accountRep": None},
                 "4": {"name": "Zephyr Energy", "accountRep": "AE B"}}
    s = runner.run_job([doc("9", client="1")], store, client(sess), True, None,
                       company_lookup=lambda ids: companies,
                       debtor_clients_lookup=lambda ids: {"9": {"1", "2", "3", "4"}})
    assert s.clients_excluded == 2 and s.clients == 2
    assert [c["client_id"] for c in sess.calls[0][1]["json"]["clients"]] == ["1", "4"]


def test_event_with_no_clients_is_still_sent_with_zero_count(store):
    sess = FakeSession(Resp(200))
    companies = {"2": {"name": "Z - Old Co", "accountRep": None}}
    runner.run_job([doc("9", client="2")], store, client(sess), True, None, company_lookup=lambda ids: companies)
    p = sess.calls[0][1]["json"]
    assert p["client_count"] == 0 and p["clients"] == [] and p["rep_groups"] == []
