import json
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

from fvdebtor_creditcheck import runner, server
from fvdebtor_creditcheck.config import Config, ConfigError
from fvdebtor_creditcheck.events import make_event_id
from fvdebtor_creditcheck.store import Store
from tests.test_job import COMPANIES, FakeSession, Resp, client, doc

TOKEN = "t" * 40
EID = make_event_id("1380", "2026-09-16")


def make_cfg(tmp_path, **kw):
    return Config(mongo_uri="mongodb://u:pw@h", webhook_url="https://h.example/x",
                  inactive_webhook_url="https://h.example/y", state_db_path=tmp_path / "s.sqlite3",
                  lock_path=tmp_path / "l.lock", log_dir=tmp_path / "logs", replay_api_token=TOKEN,
                  replay_cooldown_seconds=300, **kw)


def make_deps(sess, docs=None, linked=None):
    return server.Deps(lambda: docs if docs is not None else [doc("1380", client="2565")],
                       lambda ids: COMPANIES, lambda ids: linked or {"1380": {"2565", "1479"}},
                       lambda: client(sess))


def seed(cfg, accepted_ago_minutes=60):
    st = Store(cfg.state_db_path)
    runner.run_job([doc("1380", client="2565")], st, client(FakeSession(Resp(200))), True, None)
    old = (datetime.now(timezone.utc) - timedelta(minutes=accepted_ago_minutes)).isoformat().replace("+00:00", "Z")
    st.conn.execute("UPDATE events SET last_attempt_at=?", (old,))
    st.conn.commit()
    st.close()


def test_replay_refreshes_payload_and_keeps_event_id(tmp_path):
    cfg = make_cfg(tmp_path)
    seed(cfg)
    sess = FakeSession(Resp(200))
    status, body = server.process_replay(cfg, make_deps(sess), EID)
    assert status == 200 and body == {"status": "accepted", "event_id": EID, "payload": "refreshed"}
    sent = sess.calls[0][1]["json"]
    assert sent["event_id"] == EID and sent["client_count"] == 2
    st = Store(cfg.state_db_path)
    assert st.runs()[0]["command"] == "replay (api)"
    st.close()


def test_replay_rejects_bad_unknown_and_recent(tmp_path):
    cfg = make_cfg(tmp_path)
    seed(cfg)
    sess = FakeSession(Resp(200))
    assert server.process_replay(cfg, make_deps(sess), "nope")[0] == 400
    assert server.process_replay(cfg, make_deps(sess), None)[0] == 400
    assert server.process_replay(cfg, make_deps(sess), make_event_id("9", "2026-01-01"))[0] == 404
    seed(make_cfg(tmp_path), accepted_ago_minutes=1)
    status, body = server.process_replay(cfg, make_deps(sess), EID)
    assert status == 429 and body["status"] == "cooldown" and not sess.calls


def test_refresh_falls_back_to_stored_payload_when_mongo_no_longer_matches(tmp_path):
    cfg = make_cfg(tmp_path)
    seed(cfg)
    sess = FakeSession(Resp(200))
    status, body = server.process_replay(cfg, make_deps(sess, docs=[]), EID)
    assert status == 200 and body["payload"] == "stored" and sess.calls[0][1]["json"]["event_id"] == EID


def test_failed_delivery_returns_502_and_busy_returns_409(tmp_path):
    cfg = make_cfg(tmp_path)
    seed(cfg)
    assert server.process_replay(cfg, make_deps(FakeSession(Resp(400))), EID)[0] == 502
    with runner.single_instance(cfg.lock_path):
        assert server.process_replay(cfg, make_deps(FakeSession(Resp(200))), EID)[0] == 409


def test_token_required_and_loopback_only(tmp_path):
    with pytest.raises(ConfigError):
        Config(mongo_uri=None, webhook_url=None, inactive_webhook_url=None, state_db_path=tmp_path / "s",
               lock_path=tmp_path / "l", log_dir=tmp_path, replay_api_token="short").require_replay_token()
    cfg = make_cfg(tmp_path)
    with pytest.raises(ValueError):
        server.create_server(cfg, host="0.0.0.0", deps=make_deps(FakeSession(Resp(200))))
    assert TOKEN not in repr(cfg) and "pw" not in repr(cfg)


def _call(port, method="POST", path="/replay", token=TOKEN, body=None, raw=None, header="Authorization"):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else b"")
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data if method == "POST" else None,
                                 method=method)
    if token:
        req.add_header(header, f"Bearer {token}" if header == "Authorization" else token)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_http_endpoint_auth_routing_and_replay(tmp_path):
    cfg = make_cfg(tmp_path)
    seed(cfg)
    sess = FakeSession(Resp(200))
    httpd = server.create_server(cfg, port=0, deps=make_deps(sess))
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        assert _call(port, "GET", "/health", token=None) == (200, {"ok": True})
        assert _call(port, "GET", "/replay", token=None)[0] == 404
        assert _call(port, token=None, body={"event_id": EID})[0] == 401
        assert _call(port, token="x" * 40, body={"event_id": EID}, header="X-Webhook-Secret")[0] == 401
        assert _call(port, token="x" * 40, body={"event_id": EID})[0] == 401
        assert _call(port, path="/other", body={"event_id": EID})[0] == 404
        assert _call(port, raw=b"not json")[0] == 400
        assert _call(port, raw=b"x" * 5000)[0] == 413
        assert not sess.calls
        status, body = _call(port, body={"event_id": EID})
        assert status == 200 and body["status"] == "accepted" and len(sess.calls) == 1
        assert _call(port, body={"event_id": EID}, header="X-Webhook-Secret")[0] == 429  # same secret, other header
        assert len(sess.calls) == 1
    finally:
        httpd.shutdown()
        httpd.server_close()
