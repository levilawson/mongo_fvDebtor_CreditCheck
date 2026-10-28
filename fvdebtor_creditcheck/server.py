"""Authenticated local HTTP endpoint that replays one stored event by event_id.

Meant to sit behind a Cloudflare tunnel (the tunnel is the only thing reachable from outside; this server binds to
loopback only). A Zap calls POST /replay with `Authorization: Bearer <REPLAY_API_TOKEN>` and {"event_id": "..."}.
The event is rebuilt from current Mongo data (same event_id and detected_at) and sent to the Zapier hook again.
Delivery stays at least once: the Zap must treat a repeated event_id as a rerun on purpose.
"""
from __future__ import annotations

import hmac
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

from . import mongo_source
from .config import Config
from .runner import AlreadyRunning, rebuild_event_payload, replay_event, single_instance
from .store import Store
from .webhook import RoutedWebhook, WebhookClient

log = logging.getLogger(__name__)

EVENT_ID_RE = re.compile(r"^fvcr-[0-9a-f]{32}$")
MAX_BODY = 4096
LOOPBACK = {"127.0.0.1", "localhost", "::1"}


@dataclass
class Deps:
    fetch_expired: Callable
    fetch_companies: Callable
    fetch_debtor_clients: Callable
    build_client: Callable


def default_deps(cfg: Config) -> Deps:
    def build_client():
        def one(url):
            return WebhookClient(url, cfg.connect_timeout, cfg.read_timeout, cfg.max_attempts,
                                 cfg.backoff_base, cfg.retry_after_max)
        return RoutedWebhook(one(cfg.require_webhook()), one(cfg.require_inactive_webhook()))
    uri = cfg.require_mongo
    return Deps(lambda: mongo_source.fetch_expired(uri()),
                lambda ids: mongo_source.fetch_companies(uri(), ids),
                lambda ids: mongo_source.fetch_debtor_clients(uri(), ids),
                build_client)


def process_replay(cfg: Config, deps: Deps, event_id, refresh: bool = True, now: datetime | None = None):
    """Returns (http_status, body). Never raises for expected conditions."""
    if not isinstance(event_id, str) or not EVENT_ID_RE.match(event_id):
        return 400, {"status": "error", "error": "event_id missing or malformed"}
    now = now or datetime.now(timezone.utc)
    try:
        with single_instance(cfg.lock_path):
            store = Store(cfg.state_db_path)
            try:
                row = store.get_by_event_id(event_id)
                if row is None:
                    return 404, {"status": "error", "error": "unknown event_id"}
                last = row["last_attempt_at"]
                if last:
                    age = (now - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds()
                    if age < cfg.replay_cooldown_seconds:
                        wait = int(cfg.replay_cooldown_seconds - age) + 1
                        return 429, {"status": "cooldown", "event_id": event_id, "retry_after_seconds": wait}
                payload, used = None, "stored"
                if refresh:
                    try:
                        payload = rebuild_event_payload(
                            row, deps.fetch_expired(), deps.fetch_companies, deps.fetch_debtor_clients,
                            cfg.activity_window_days, cfg.report_validity_days, cfg.exclude_pattern())
                        used = "refreshed"
                    except (LookupError, mongo_source.MongoReadError) as exc:
                        log.warning("refresh unavailable for event_id=%s (%s); replaying stored payload",
                                    event_id, type(exc).__name__)
                store.start_run("replay (api)", {"event_id": event_id, "payload": used})
                summary = replay_event(store, deps.build_client(), event_id, payload)
                store.finish_run({"accepted": summary.accepted, "failed": summary.failed}, summary.aborted)
            finally:
                store.close()
    except AlreadyRunning:
        return 409, {"status": "busy", "error": "another run is in progress; try again shortly"}
    if summary.accepted:
        return 200, {"status": "accepted", "event_id": event_id, "payload": used}
    return 502, {"status": "failed", "event_id": event_id, "payload": used,
                 "error": summary.aborted or "delivery to Zapier failed"}


def make_handler(cfg: Config, deps: Deps, token: str):
    token_b = token.encode()

    class Handler(BaseHTTPRequestHandler):
        timeout = 30
        server_version = "fv-replay"

        def log_message(self, fmt, *args):  # request line + status only; headers (token) are never logged
            log.info("http " + fmt, *args)

        def _send(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            if status == 429:
                self.send_header("Retry-After", str(body.get("retry_after_seconds", 60)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                return self._send(200, {"ok": True})
            self._send(404, {"status": "error", "error": "not found"})

        def do_POST(self):
            if self.path != "/replay":
                return self._send(404, {"status": "error", "error": "not found"})
            supplied = self.headers.get("Authorization", "")
            if not (supplied.startswith("Bearer ")
                    and hmac.compare_digest(supplied[7:].strip().encode(), token_b)):
                log.warning("rejected replay request: bad or missing token")
                return self._send(401, {"status": "error", "error": "unauthorized"})
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                return self._send(411, {"status": "error", "error": "Content-Length required"})
            if length > MAX_BODY:
                return self._send(413, {"status": "error", "error": "body too large"})
            try:
                data = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(data, dict):
                    raise ValueError
            except ValueError:
                return self._send(400, {"status": "error", "error": "body must be a JSON object"})
            refresh = data.get("refresh", True) is not False
            status, body = process_replay(cfg, deps, data.get("event_id"), refresh)
            log.info("replay request event_id=%s -> %s", data.get("event_id") if status != 400 else "?", status)
            self._send(status, body)

    return Handler


def create_server(cfg: Config, host: str = "127.0.0.1", port: int | None = None, deps: Deps | None = None):
    if host not in LOOPBACK:
        raise ValueError("the replay server only binds to loopback; expose it through the tunnel")
    token = cfg.require_replay_token()
    return HTTPServer((host, cfg.replay_api_port if port is None else port),
                      make_handler(cfg, deps or default_deps(cfg), token))
