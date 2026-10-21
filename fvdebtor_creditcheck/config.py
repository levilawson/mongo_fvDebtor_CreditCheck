"""Configuration from environment (optionally a .env file next to the project)."""
from __future__ import annotations

import os
from dataclasses import dataclass
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
    mongo_uri: str | None
    webhook_url: str | None
    state_db_path: Path
    lock_path: Path
    log_dir: Path
    connect_timeout: float = 5.0
    read_timeout: float = 20.0
    max_attempts: int = 4
    backoff_base: float = 2.0
    retry_after_max: float = 60.0
    max_consecutive_failed_events: int = 3

    def require_mongo(self) -> str:
        if not self.mongo_uri:
            raise ConfigError("MONGO_URI is not set")
        return self.mongo_uri

    def require_webhook(self) -> str:
        if not self.webhook_url:
            raise ConfigError("ZAPIER_WEBHOOK_URL is not set")
        if urlparse(self.webhook_url).scheme != "https":
            raise ConfigError("ZAPIER_WEBHOOK_URL must use https")
        return self.webhook_url


def _path(name: str, default: str) -> Path:
    p = Path(os.environ.get(name) or default)
    return p if p.is_absolute() else PROJECT_ROOT / p


def load_config() -> Config:
    load_dotenv(PROJECT_ROOT / ".env")
    try:
        return Config(
            mongo_uri=os.environ.get("MONGO_URI") or None,
            webhook_url=os.environ.get("ZAPIER_WEBHOOK_URL") or None,
            state_db_path=_path("STATE_DB_PATH", "state/deliveries.sqlite3"),
            lock_path=_path("LOCK_PATH", "state/job.lock"),
            log_dir=_path("LOG_DIR", "logs"),
            connect_timeout=float(os.environ.get("WEBHOOK_CONNECT_TIMEOUT", 5)),
            read_timeout=float(os.environ.get("WEBHOOK_READ_TIMEOUT", 20)),
            max_attempts=int(os.environ.get("WEBHOOK_MAX_ATTEMPTS", 4)),
            backoff_base=float(os.environ.get("WEBHOOK_BACKOFF_BASE_SECONDS", 2)),
            retry_after_max=float(os.environ.get("WEBHOOK_RETRY_AFTER_MAX_SECONDS", 60)),
            max_consecutive_failed_events=int(os.environ.get("MAX_CONSECUTIVE_FAILED_EVENTS", 3)),
        )
    except ValueError as exc:
        raise ConfigError(f"invalid numeric setting: {exc}") from None
