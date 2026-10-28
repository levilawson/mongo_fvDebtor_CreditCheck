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
                      "credit_score", "credit_report_date", "credit_average_days", "credit_report_expired", "is_active", "credit_report_expires_on",
                      "restricted", "mongo_updated_at", "client_count", "clients", "rep_groups", "payload_json"}


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
        {"client_id": "1479", "client_name": "Other Client", "client_label": "Other Client (FV ID: 1479)",
         "account_rep": None},
        {"client_id": "2565", "client_name": "Raine Energy Services LLC",
         "client_label": "Raine Energy Services LLC (FV ID: 2565)", "account_rep": "Christine Dunn"},
        {"client_id": "9999", "client_name": None, "client_label": "Unnamed client (FV ID: 9999)",
         "account_rep": None}]


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
    monkeypatch.setattr(cli, "fetch_active_debtors", lambda uri, since: {"1892"})
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
    from fvdebtor_creditcheck.events import client_label
    clients = [
        {"client_id": "2565", "client_name": "Raine Energy LLC", "account_rep": "AE A"},
        {"client_id": "1", "client_name": "Client ABC", "account_rep": "AE A"},
        {"client_id": "2", "client_name": "Client BCD", "account_rep": "AE A"},
        {"client_id": "3", "client_name": "Client CDE", "account_rep": "AE B"},
        {"client_id": "4", "client_name": None, "account_rep": None},
    ]
    for c in clients:
        c["client_label"] = client_label(c["client_id"], c["client_name"])
    g = build_rep_groups(clients)
    assert [x["account_rep"] for x in g] == ["AE A", "AE B", None]
    assert g[0]["clients_text"] == "Raine Energy LLC (FV ID: 2565), Client ABC (FV ID: 1), and Client BCD (FV ID: 2)"
    assert g[1]["clients_text"] == "Client CDE (FV ID: 3)" and g[1]["client_count"] == 1
    assert g[2]["clients_text"] == "Unnamed client (FV ID: 4)"


def test_z_and_test_clients_excluded_but_lookalikes_kept(store):
    sess = FakeSession(Resp(200))
    companies = {"1": {"name": "Good Co", "accountRep": "AE A"},
                 "2": {"name": "Z - Old Co", "accountRep": None},
                 "3": {"name": " z-Old Co 2", "accountRep": None},
                 "4": {"name": "Zephyr Energy", "accountRep": "AE B"},
                 "5": {"name": "Test Company", "accountRep": None},
                 "6": {"name": "test - ABC", "accountRep": None},
                 "7": {"name": "Testa Energy", "accountRep": "AE C"},
                 "8": {"name": "Texas Test & Inspection LLC", "accountRep": "AE D"},
                 "9": {"name": "Demo Oil", "accountRep": None}}
    s = runner.run_job([doc("9", client="1")], store, client(sess), True, None,
                       company_lookup=lambda ids: companies,
                       debtor_clients_lookup=lambda ids: {"9": set(companies)},
                       exclude_regex=__import__("re").compile(r"^\s*demo\b", __import__("re").I))
    assert s.clients_excluded == 5 and s.clients == 4
    assert [c["client_id"] for c in sess.calls[0][1]["json"]["clients"]] == ["1", "4", "7", "8"]


def test_event_with_no_clients_is_still_sent_with_zero_count(store):
    sess = FakeSession(Resp(200))
    companies = {"2": {"name": "Z - Old Co", "accountRep": None}}
    runner.run_job([doc("9", client="2")], store, client(sess), True, None, company_lookup=lambda ids: companies)
    p = sess.calls[0][1]["json"]
    assert p["client_count"] == 0 and p["clients"] == [] and p["rep_groups"] == []


# ---- activity routing ----
def test_inactive_debtor_gets_inactive_event_type_flag_and_separate_key(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1"), doc("2")], store, client(sess), True, None, active_lookup=lambda: {"2"})
    by_debtor = {c[1]["json"]["debtor_id"]: c[1]["json"] for c in sess.calls}
    assert by_debtor["2"]["is_active"] is True and by_debtor["2"]["event_type"] == "factorview.credit_report_expired"
    p = by_debtor["1"]
    assert p["is_active"] is False and p["event_type"] == "factorview.credit_report_expired_inactive"
    assert "180 days" in p["inactive_reason"] and p["inactivity_window_days"] == 180
    assert p["event_id"] != make_event_id("1", "2026-09-16")
    assert store.get("1|2026-09-16|inactive")["status"] == "accepted" and store.get("2|2026-09-16")["status"] == "accepted"


def test_debtor_becoming_active_sends_active_event_after_inactive_was_sent(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1")], store, client(sess), True, None, active_lookup=lambda: set() | {"x"})
    s = runner.run_job([doc("1")], store, client(sess), True, None, active_lookup=lambda: {"1"})
    assert s.accepted == 1 and sess.calls[1][1]["json"]["is_active"] is True
    s = runner.run_job([doc("1")], store, client(sess), True, None, active_lookup=lambda: {"1"})
    assert s.already_sent == 1 and len(sess.calls) == 2


def test_inactive_events_still_carry_clients_and_reps(store):
    sess = FakeSession(Resp(200))
    runner.run_job([doc("1", client="2565")], store, client(sess), True, None,
                   company_lookup=lambda ids: COMPANIES, active_lookup=lambda: {"x"})
    p = sess.calls[0][1]["json"]
    assert p["is_active"] is False and p["rep_groups"][0]["account_rep"] == "Christine Dunn"


def test_routed_webhook_picks_destination_by_is_active():
    a, b = FakeSession(Resp(200)), FakeSession(Resp(200))
    r = webhook.RoutedWebhook(client(a), webhook.WebhookClient("https://other.example/y", 1, 2, 1, 1, 1, session=b))
    r.send({"is_active": True})
    r.send({"is_active": False})
    assert len(a.calls) == 1 and len(b.calls) == 1 and b.calls[0][0] == "https://other.example/y"


def test_active_debtor_query_and_empty_window_is_refused():
    seen = {}

    class Coll:
        def __init__(self, ids): self.ids = ids
        def distinct(self, field, q):
            seen["field"], seen["q"] = field, q
            return self.ids

    class Db:
        def __init__(self, ids): self.ids = ids
        def __getitem__(self, n):
            seen["coll"] = n
            return Coll(self.ids)

    def factory(ids):
        class Cl:
            def __init__(self, uri, **kw): pass
            def __getitem__(self, n): return Db(ids)
            def close(self): pass
        return Cl

    out = mongo_source.fetch_active_debtors("u", "2026-07-24", factory(["1892", 3442, None]))
    assert out == {"1892", "3442"} and seen["coll"] == "fvInvoice" and seen["field"] == "doc.debtorId"
    assert seen["q"] == {"doc.isPurchase": True, "doc.buyDate": {"$gte": "2026-07-24"}}
    with pytest.raises(mongo_source.SuspiciousActivityData):
        mongo_source.fetch_active_debtors("u", "2026-07-24", factory([]))


# ---- reconcile ----
def _sent(store, *debtors, active=lambda: {"1", "2", "3"}):
    runner.run_job([doc(d, "2026-09-16", score="71/A") for d in debtors], store,
                   client(FakeSession(Resp(200))), True, None, active_lookup=active)


def test_reconcile_records_new_score_when_newer_report_appears(store):
    _sent(store, "1", "2")
    ratings = {"1": [{"score": "72/A", "date": "2026-10-20", "updatedAt": "2026-10-21T00:00:00Z"},
                     {"score": "60/C", "date": "2026-09-16", "updatedAt": "2026-10-22T00:00:00Z"}],
               "2": [{"score": "71/A", "date": "2026-09-16", "updatedAt": "2026-10-22T00:00:00Z"}]}
    s = runner.reconcile(store, lambda ids: ratings)
    assert (s.checked, s.resolved, s.unresolved) == (2, 1, 1)
    row = store.get("1|2026-09-16")
    assert row["new_score"] == "72/A" and row["new_report_date"] == "2026-10-20" and row["resolved_at"]
    assert store.get("2|2026-09-16")["resolved_at"] is None
    # resolved events are not checked again
    assert runner.reconcile(store, lambda ids: ratings).checked == 1


def test_reconcile_flags_stale_unresolved_and_ignores_inactive_route_and_failed(store):
    _sent(store, "1")
    _sent(store, "2", active=lambda: {"x"})  # inactive route: no refresh expected
    runner.run_job([doc("3")], store, client(FakeSession(Resp(400))), True, None)  # failed, not accepted
    later = datetime(2099, 1, 1, tzinfo=timezone.utc)
    s = runner.reconcile(store, lambda ids: {}, stale_days=3, now=later)
    assert (s.checked, s.stale) == (1, 1)


def test_reconcile_skips_malformed_dates_and_handles_nothing_to_do(store):
    _sent(store, "1")
    s = runner.reconcile(store, lambda ids: {"1": [{"score": "9/A", "date": "2026-13-99", "updatedAt": None}]})
    assert s.resolved == 0 and s.unresolved == 1
    assert runner.reconcile(Store(None), lambda ids: {}).checked == 0


def test_existing_database_without_new_columns_is_migrated(tmp_path):
    import sqlite3
    path = tmp_path / "old.sqlite3"
    c = sqlite3.connect(path)
    c.executescript(__import__("fvdebtor_creditcheck.store", fromlist=["SCHEMA"]).SCHEMA)
    c.execute("INSERT INTO events VALUES ('3442|2026-09-18','fvcr-x','3442','2026-09-18',?, 'accepted',1,"
              "'2026-10-21T00:00:00Z',NULL,'2026-10-21T00:00:00Z',200,NULL)", (json.dumps({"credit_score": "57B"}),))
    c.commit(); c.close()
    s = Store(path)
    assert s.get("3442|2026-09-18")["resolved_at"] is None
    assert len(s.unresolved_accepted()) == 1
    s.close()


def test_fetch_current_ratings_query_is_read_only_lookup_by_debtor():
    seen = {}

    class Coll:
        def find(self, q, p):
            seen["q"] = q
            return [{"doc": {"id": "1", "creditRating": {"score": "72/A", "date": "2026-10-20"}}, "updatedAt": "u"}]

    class Db:
        def __getitem__(self, n): return Coll()

    class Cl:
        def __init__(self, uri, **kw): pass
        def __getitem__(self, n): return Db()
        def close(self): pass

    out = mongo_source.fetch_current_ratings("u", ["1"], Cl)
    assert seen["q"] == {"doc.id": {"$in": ["1", 1]}} and out["1"][0]["score"] == "72/A"


def test_cli_reconcile_and_status(tmp_path, monkeypatch, capsys):
    from fvdebtor_creditcheck import cli
    for k, v in {"MONGO_URI": "mongodb://u:pw@h", "ZAPIER_WEBHOOK_URL": "https://h.example/x",
                 "STATE_DB_PATH": str(tmp_path / "s.sqlite3"), "LOCK_PATH": str(tmp_path / "l.lock"),
                 "LOG_DIR": str(tmp_path / "logs")}.items():
        monkeypatch.setenv(k, v)
    st = Store(tmp_path / "s.sqlite3")
    _sent(st, "1")
    st.close()
    monkeypatch.setattr(cli, "fetch_current_ratings",
                        lambda uri, ids: {"1": [{"score": "72/A", "date": "2026-10-20", "updatedAt": None}]})
    assert cli.main(["status", "--unresolved"]) == 0
    assert "score=71/A->?" in capsys.readouterr().out
    assert cli.main(["reconcile"]) == 0
    assert "resolved=1" in capsys.readouterr().out
    assert cli.main(["status"]) == 0
    assert "score=71/A->72/A" in capsys.readouterr().out


def test_readonly_status_of_unmigrated_db_lists_accepted_as_unresolved(tmp_path):
    import sqlite3
    path = tmp_path / "old.sqlite3"
    c = sqlite3.connect(path)
    c.executescript(__import__("fvdebtor_creditcheck.store", fromlist=["SCHEMA"]).SCHEMA)
    c.execute("INSERT INTO events VALUES ('3442|2026-09-18','fvcr-x','3442','2026-09-18',?, 'accepted',1,"
              "'2026-10-21T00:00:00Z',NULL,'2026-10-21T00:00:00Z',200,NULL)", (json.dumps({"credit_score": "57B"}),))
    c.commit(); c.close()
    ro = Store(path, readonly=True)
    assert [r["debtor_id"] for r in ro.unresolved_accepted()] == ["3442"]
    ro.close()


# ---- rollout cohort filters ----
TODAY = datetime(2026, 10, 23, tzinfo=timezone.utc).date()


def test_expired_within_days_is_report_date_plus_validity(store):
    sess = FakeSession(Resp(200))
    # today 2026-10-23, 30-day validity, window 30 -> expiry must be within 2026-09-23..2026-10-23
    docs = [doc("1", "2026-09-23"), doc("2", "2026-08-24"), doc("3", "2026-08-23"), doc("4", "2026-09-24"),
            doc("5", "2026-10-20")]
    s = runner.run_job(docs, store, client(sess), False, None, expired_within_days=30, today=TODAY)
    assert s.would_send == ["1", "2"] and s.skipped_expired_window == 3
    s = runner.run_job(docs, store, client(sess), False, None, expired_within_days=30, today=TODAY, validity_days=10)
    assert s.would_send == ["1", "4"]  # 10-day validity: expiries 10-03 and 10-04 are in range; 5 expires in the future


def test_payload_has_expiry_date_from_validity():
    p = collect_candidates([doc("1", "2026-09-18")]).candidates[0].build_payload("t")
    assert p["credit_report_expires_on"] == "2026-10-18"
    assert collect_candidates([doc("1", "2026-09-18")]).candidates[0].build_payload("t", None, 180, 45)["credit_report_expires_on"] == "2026-11-02"


def test_purchased_within_days_skips_inactive_instead_of_routing(store):
    sess = FakeSession(Resp(200))
    s = runner.run_job([doc("1"), doc("2")], store, client(sess), True, None,
                       active_lookup=lambda: {"2"}, skip_inactive=True)
    assert s.skipped_inactive == 1 and s.accepted == 1 and s.inactive == 0
    assert [c[1]["json"]["debtor_id"] for c in sess.calls] == ["2"]
    assert store.get("1|2026-09-16|inactive") is None and store.get("1|2026-09-16") is None


def test_limit_applies_after_cohort_filters(store):
    sess = FakeSession(Resp(200))
    docs = [doc("1", "2026-01-01"), doc("2", "2026-01-01"), doc("3", "2026-09-20")]
    s = runner.run_job(docs, store, client(sess), True, 1, expired_within_days=30, today=TODAY)
    assert s.accepted == 1 and sess.calls[0][1]["json"]["debtor_id"] == "3"


def test_cli_cohort_flags(tmp_path, monkeypatch, capsys):
    from fvdebtor_creditcheck import cli
    for k, v in {"MONGO_URI": "mongodb://u:pw@h", "STATE_DB_PATH": str(tmp_path / "s.sqlite3"),
                 "LOCK_PATH": str(tmp_path / "l.lock"), "LOG_DIR": str(tmp_path / "logs")}.items():
        monkeypatch.setenv(k, v)
    recent = (datetime.now(timezone.utc).date() - __import__("datetime").timedelta(days=45)).isoformat()
    monkeypatch.setattr(cli, "fetch_expired", lambda uri: [doc("1", recent), doc("2", recent), doc("3", "2020-01-01")])
    monkeypatch.setattr(cli, "fetch_companies", lambda uri, ids: COMPANIES)
    monkeypatch.setattr(cli, "fetch_debtor_clients", lambda uri, ids: {})
    seen = {}
    monkeypatch.setattr(cli, "fetch_active_debtors", lambda uri, since: seen.setdefault("since", since) and {"1"})
    assert cli.main(["run", "--expired-within-days", "30", "--purchased-within-days", "30"]) == 0
    out = capsys.readouterr().out
    assert "['1']" in out and "skipped_expired_window=1" in out and "skipped_inactive=1" in out
    assert cli.main(["run", "--expired-within-days", "0"]) == 2


# ---- local history ----
def _env(monkeypatch, tmp_path):
    for k, v in {"MONGO_URI": "mongodb://u:pw@h", "ZAPIER_WEBHOOK_URL": "https://h.example/x",
                 "ZAPIER_INACTIVE_WEBHOOK_URL": "https://h.example/y",
                 "STATE_DB_PATH": str(tmp_path / "s.sqlite3"), "LOCK_PATH": str(tmp_path / "l.lock"),
                 "LOG_DIR": str(tmp_path / "logs")}.items():
        monkeypatch.setenv(k, v)


def test_attempts_and_scores_are_recorded_with_run_id(store):
    rid = store.start_run("run --send", {"limit": 1})
    runner.run_job([doc("1", score="71/A")], store, client(FakeSession(Resp(500), Resp(200)), attempts=1), True, None)
    runner.run_job([doc("1", score="71/A")], store, client(FakeSession(Resp(200))), True, None)
    store.finish_run({"accepted": 1})
    h = store.debtor_history("1")
    assert [a["outcome"] for a in h["attempts"]] == ["failed", "accepted"] and h["attempts"][0]["run_id"] == rid
    assert [(s["score"], s["source"]) for s in h["scores"]] == [("71/A", "detected")]
    assert store.runs()[0]["finished_at"] and "accepted" in store.runs()[0]["summary_json"]


def test_refreshed_score_added_to_history_on_reconcile(store):
    _sent(store, "1")
    runner.reconcile(store, lambda ids: {"1": [{"score": "72/A", "date": "2026-10-20", "updatedAt": None}]})
    assert [(s["score"], s["source"]) for s in store.debtor_history("1")["scores"]] == \
        [("71/A", "detected"), ("72/A", "refreshed")]


def test_backfill_seeds_scores_for_pre_history_events(tmp_path):
    import sqlite3
    path = tmp_path / "old.sqlite3"
    c = sqlite3.connect(path)
    c.executescript("""CREATE TABLE events (event_key TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
        debtor_id TEXT NOT NULL, report_date TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, first_seen_at TEXT NOT NULL, last_attempt_at TEXT, accepted_at TEXT,
        last_http_status INTEGER, last_error TEXT);""")
    c.execute("INSERT INTO events VALUES ('3442|2026-09-18','fvcr-x','3442','2026-09-18',?, 'accepted',1,"
              "'2026-10-21T00:00:00Z',NULL,'2026-10-21T00:00:00Z',200,NULL)", (json.dumps({"credit_score": "57B"}),))
    c.commit(); c.close()
    s = Store(path)
    assert [(r["score"], r["source"]) for r in s.debtor_history("3442")["scores"]] == [("57B", "detected")]
    s.close()
    s = Store(path)  # idempotent
    assert len(s.debtor_history("3442")["scores"]) == 1
    s.close()


def test_cli_runs_history_export_backup(tmp_path, monkeypatch, capsys):
    import csv as _csv
    from fvdebtor_creditcheck import cli
    _env(monkeypatch, tmp_path)
    monkeypatch.setattr(cli, "fetch_expired", lambda uri: [doc("1892", "2026-09-08", score="71/A", client="2565")])
    monkeypatch.setattr(cli, "fetch_companies", lambda uri, ids: COMPANIES)
    monkeypatch.setattr(cli, "fetch_debtor_clients", lambda uri, ids: {})
    monkeypatch.setattr(cli, "fetch_active_debtors", lambda uri, since: {"1892"})
    monkeypatch.setattr(cli, "build_client", lambda cfg: client(FakeSession(Resp(200))))
    assert cli.main(["run", "--send", "--debtor-id", "1892"]) == 0
    capsys.readouterr()
    assert cli.main(["runs"]) == 0
    out = capsys.readouterr().out
    assert "run --send" in out and "accepted=1" in out and "mongodb" not in out and "h.example" not in out
    assert cli.main(["history", "1892"]) == 0
    out = capsys.readouterr().out
    assert "score=71/A" in out and "(detected)" in out and "accepted" in out
    csv_path = tmp_path / "e.csv"
    assert cli.main(["export", "--out", str(csv_path)]) == 0
    rows = list(_csv.DictReader(open(csv_path, encoding="utf-8")))
    assert rows[0]["debtor_id"] == "1892" and rows[0]["old_score"] == "71/A" and "Christine Dunn" in rows[0]["clients"]
    assert rows[0]["expires_on"] == "2026-10-08"
    bk = tmp_path / "b.sqlite3"
    assert cli.main(["backup", "--out", str(bk)]) == 0
    restored = Store(bk)
    assert restored.get("1892|2026-09-08")["status"] == "accepted"
    restored.close()


def test_z_prefix_variants_all_excluded():
    from fvdebtor_creditcheck.events import is_excluded_client as ex
    assert all(ex(n) for n in ["Z - Old", "Z- Old", "Z-Old", " z -Old", "z-"])
    assert not any(ex(n) for n in ["Zephyr", "Z Energy", "Zeta-Co", "Testa"])


def test_client_label_on_same_line_and_lines_per_rep():
    p = collect_candidates([doc("9", client="2565")]).candidates[0].build_payload("t", COMPANIES)
    assert p["clients"][0]["client_label"] == "Raine Energy Services LLC (FV ID: 2565)"
    g = p["rep_groups"][0]
    assert g["clients"][0]["client_label"] == "Raine Energy Services LLC (FV ID: 2565)"
    assert g["clients_lines"] == "Raine Energy Services LLC (FV ID: 2565)"
    from fvdebtor_creditcheck.events import build_rep_groups
    two = build_rep_groups([{"client_id": "1", "client_name": "A", "client_label": "A (FV ID: 1)", "account_rep": "X"},
                            {"client_id": "2", "client_name": "B", "client_label": "B (FV ID: 2)", "account_rep": "X"}])
    assert two[0]["clients_lines"] == "A (FV ID: 1)\nB (FV ID: 2)"


def test_payload_json_is_the_whole_event_as_one_string():
    p = collect_candidates([doc("1892", client="2565")]).candidates[0].build_payload("t", COMPANIES)
    inner = json.loads(p["payload_json"])
    assert "payload_json" not in inner and inner["event_id"] == p["event_id"]
    assert inner["rep_groups"] == p["rep_groups"] and inner["credit_score"] == p["credit_score"]
    assert inner == {k: v for k, v in p.items() if k != "payload_json"}
