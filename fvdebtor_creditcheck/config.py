"""Configuration from environment (optionally a .env file next to the project)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MONGO_DB = "payments-service"
MONGO_COLLECTION = "fvDebtor"


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    # Secrets are kept out of repr() so they can't leak into logs or tracebacks.
    mongo_uri: str | None = field(repr=False)
    webhook_url: str | None = field(repr=False)
    inactive_webhook_url: str | None = field(repr=False)
    state_db_path: Path
    lock_path: Path
    log_dir: Path
    connect_timeout: float = 5.0
    read_timeout: float = 20.0
    max_attempts: int = 4
    backoff_base: float = 2.0
    retry_after_max: float = 60.0
    max_consecutive_failed_events: int = 3
    activity_window_days: int = 180
    reconcile_stale_days: int = 3
    report_validity_days: int = 30
    client_exclude_regex: str | None = None
    replay_api_token: str | None = field(default=None, repr=False)
    replay_api_port: int = 8787
    replay_cooldown_seconds: int = 300

    def require_mongo(self) -> str:
        if not self.mongo_uri:
            raise ConfigError("MONGO_URI is not set")
        return self.mongo_uri

    def require_webhook(self) -> str:
        return self._https("ZAPIER_WEBHOOK_URL", self.webhook_url)

    def require_inactive_webhook(self) -> str:
        return self._https("ZAPIER_INACTIVE_WEBHOOK_URL", self.inactive_webhook_url)

    def exclude_pattern(self):
        if not self.client_exclude_regex:
            return None
        try:
            return re.compile(self.client_exclude_regex, re.IGNORECASE)
        except re.error:
            raise ConfigError("CLIENT_EXCLUDE_REGEX is not a valid regular expression") from None

    def require_replay_token(self) -> str:
        if not self.replay_api_token or len(self.replay_api_token) < 32:
            raise ConfigError("REPLAY_API_TOKEN must be set to a random value of at least 32 characters")
        return self.replay_api_token

    @staticmethod
    def _https(name: str, url: str | None) -> str:
        if not url:
            raise ConfigError(f"{name} is not set")
        if urlparse(url).scheme != "https":
            raise ConfigError(f"{name} must use https")
        return url


def _path(name: str, default: str) -> Path:
    p = Path(os.environ.get(name) or default)
    return p if p.is_absolute() else PROJECT_ROOT / p


def load_config() -> Config:
    load_dotenv(PROJECT_ROOT / ".env")
    try:
        return Config(
            mongo_uri=os.environ.get("MONGO_URI") or None,
            webhook_url=os.environ.get("ZAPIER_WEBHOOK_URL") or None,
            inactive_webhook_url=os.environ.get("ZAPIER_INACTIVE_WEBHOOK_URL") or None,
            state_db_path=_path("STATE_DB_PATH", "state/deliveries.sqlite3"),
            lock_path=_path("LOCK_PATH", "state/job.lock"),
            log_dir=_path("LOG_DIR", "logs"),
            connect_timeout=float(os.environ.get("WEBHOOK_CONNECT_TIMEOUT", 5)),
            read_timeout=float(os.environ.get("WEBHOOK_READ_TIMEOUT", 20)),
            max_attempts=int(os.environ.get("WEBHOOK_MAX_ATTEMPTS", 4)),
            backoff_base=float(os.environ.get("WEBHOOK_BACKOFF_BASE_SECONDS", 2)),
            retry_after_max=float(os.environ.get("WEBHOOK_RETRY_AFTER_MAX_SECONDS", 60)),
            max_consecutive_failed_events=int(os.environ.get("MAX_CONSECUTIVE_FAILED_EVENTS", 3)),
            activity_window_days=int(os.environ.get("ACTIVITY_WINDOW_DAYS", 180)),
            reconcile_stale_days=int(os.environ.get("RECONCILE_STALE_DAYS", 3)),
            report_validity_days=int(os.environ.get("REPORT_VALIDITY_DAYS", 30)),
            client_exclude_regex=os.environ.get("CLIENT_EXCLUDE_REGEX") or None,
            replay_api_token=os.environ.get("REPLAY_API_TOKEN") or None,
            replay_api_port=int(os.environ.get("REPLAY_API_PORT", 8787)),
            replay_cooldown_seconds=int(os.environ.get("REPLAY_COOLDOWN_SECONDS", 300)),
        )
    except ValueError as exc:
        raise ConfigError(f"invalid numeric setting: {exc}") from None
