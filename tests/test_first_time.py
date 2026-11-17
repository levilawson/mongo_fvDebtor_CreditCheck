from fvdebtor_creditcheck import runner, server
from fvdebtor_creditcheck.events import NEVER, make_event_id
from fvdebtor_creditcheck.store import Store
from tests.test_job import FakeSession, Resp, client, doc
from tests.test_server import COMPANIES, make_cfg, make_deps


def never_doc(i="3542", client_id="3378"):
    return {"clientId": client_id, "doc": {"id": i, "name": "Markhor", "restricted": False,
            "creditRating": {"averageDays": 0}, "creditReportExpired": False}, "updatedAt": "2026-11-17T07:14:23Z"}


def deps_for(sess, docs, active=()):
    d = make_deps(sess)
    d.fetch_debtor_docs = lambda debtor_id: docs
    d.fetch_active = lambda: set(active) or {"999"}
    d.fetch_debtor_clients = lambda ids: {}
    d.fetch_companies = lambda ids: COMPANIES
    return d


def test_never_pulled_debtor_is_sent_for_the_first_time(tmp_path):
    cfg = make_cfg(tmp_path)
    sess = FakeSession(Resp(200))
    status, body = server.process_first_time(cfg, deps_for(sess, [never_doc()]), "3542")
    assert status == 200 and body["never_pulled"] is True
    sent = sess.calls[0][1]["json"]
    assert sent["credit_report_date"] is None and sent["credit_report_expires_on"] is None
    assert sent["never_pulled"] is True and sent["debtor_id"] == "3542"
    assert sent["event_id"] == make_event_id("3542", NEVER, False)  # inactive route (no recent purchases)
    st = Store(cfg.state_db_path)
    assert st.get_by_event_id(sent["event_id"])["status"] == "accepted"
    st.close()


def test_second_click_respects_cooldown_and_unknown_debtor_is_404(tmp_path):
    cfg = make_cfg(tmp_path)
    sess = FakeSession(Resp(200), Resp(200))
    assert server.process_first_time(cfg, deps_for(sess, [never_doc()]), "3542")[0] == 200
    assert server.process_first_time(cfg, deps_for(sess, [never_doc()]), "3542")[0] == 429
    assert len(sess.calls) == 1
    assert server.process_first_time(cfg, deps_for(sess, []), "777")[0] == 404
    assert server.process_first_time(cfg, deps_for(sess, []), "bad id!")[0] == 400


def test_reconcile_resolves_never_pulled_event_when_first_report_appears(tmp_path):
    cfg = make_cfg(tmp_path)
    server.process_first_time(cfg, deps_for(FakeSession(Resp(200)), [never_doc()], active={"3542"}), "3542")
    st = Store(cfg.state_db_path)
    ratings = lambda ids: {"3542": [{"score": "65/C", "date": "2026-11-18", "updatedAt": None}]}
    s = runner.reconcile(st, ratings)
    assert s.resolved == 1
    st.close()
