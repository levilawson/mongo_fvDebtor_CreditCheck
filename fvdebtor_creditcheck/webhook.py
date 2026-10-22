"""Zapier Catch Hook delivery with bounded retries.

Delivery is AT LEAST ONCE. A read timeout can occur after Zapier accepted the
request, so a later retry/replay may duplicate it; event_id is stable so the Zap
must deduplicate on it.
"""
from __future__ import annotations

import email.utils
import logging
import time
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)

ACCEPTED = "accepted"
FAILED = "failed"            # retries exhausted or permanent per-event 4xx; keep for later
CONFIG_ERROR = "config_error"  # destination problem: stop the whole run

RETRYABLE_STATUS = {408, 429}
CONFIG_STATUS = {401, 403, 404, 410}


@dataclass
class Result:
    outcome: str
    attempts: int
    http_status: int | None = None
    error: str | None = None


class RoutedWebhook:
    """Sends inactive-debtor events (is_active == False) to the second webhook, everything else to the first."""

    def __init__(self, active: "WebhookClient", inactive: "WebhookClient"):
        self._active, self._inactive = active, inactive

    def send(self, payload: dict) -> Result:
        return (self._inactive if payload.get("is_active") is False else self._active).send(payload)


def parse_retry_after(value: str | None, cap: float) -> float | None:
    if not value:
        return None
    value = value.strip()
    try:
        secs = float(value)
    except ValueError:
        try:
            dt = email.utils.parsedate_to_datetime(value)
            secs = dt.timestamp() - time.time()
        except (TypeError, ValueError):
            return None
    return max(0.0, min(secs, cap))


class WebhookClient:
    def __init__(self, url: str, connect_timeout: float, read_timeout: float, max_attempts: int,
                 backoff_base: float, retry_after_max: float, session=None, sleep=time.sleep):
        self._url = url
        self._timeout = (connect_timeout, read_timeout)
        self._max_attempts = max(1, max_attempts)
        self._backoff = backoff_base
        self._retry_after_max = retry_after_max
        self._session = session or requests.Session()
        self._sleep = sleep

    def send(self, payload: dict) -> Result:
        last_status: int | None = None
        last_error = None
        for attempt in range(1, self._max_attempts + 1):
            wait = None
            try:
                resp = self._session.post(self._url, json=payload, timeout=self._timeout,
                                          verify=True, allow_redirects=False)
            except requests.Timeout:
                # Zapier may have accepted it; retry is safe only because event_id is stable.
                last_status, last_error = None, "timeout (delivery outcome unknown)"
            except requests.RequestException as exc:
                # Class name only: exception text can embed the webhook URL.
                last_status, last_error = None, f"connection error: {type(exc).__name__}"
            else:
                status = resp.status_code
                if 200 <= status < 300:
                    return Result(ACCEPTED, attempt, status)
                last_status, last_error = status, f"HTTP {status}"
                if status in CONFIG_STATUS or 300 <= status < 400:
                    return Result(CONFIG_ERROR, attempt, status, f"destination rejected request: HTTP {status}")
                if status in RETRYABLE_STATUS or status >= 500:
                    wait = parse_retry_after(resp.headers.get("Retry-After"), self._retry_after_max)
                else:
                    return Result(FAILED, attempt, status, f"permanent HTTP {status}")
            if attempt < self._max_attempts:
                if wait is None:
                    wait = min(self._backoff * (2 ** (attempt - 1)), self._retry_after_max)
                log.warning("webhook attempt %d/%d failed (%s); retrying in %.1fs",
                            attempt, self._max_attempts, last_error, wait)
                self._sleep(wait)
        return Result(FAILED, self._max_attempts, last_status, last_error)
