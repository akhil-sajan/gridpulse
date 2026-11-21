"""HTTP client with retries and exponential backoff.

Retries on network errors, timeouts, HTTP 429 and 5xx. Honors ``Retry-After``.
Does not retry other 4xx errors: those mean the request itself is wrong.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

import requests

from ingestion.common.config import HttpSettings
from ingestion.common.logs import get_logger

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
RETRYABLE_EXCEPTIONS = (requests.ConnectionError, requests.Timeout)

log = get_logger(__name__)


class HttpRequestError(RuntimeError):
    """Raised when a request fails permanently or runs out of retries."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def backoff_delay(
    attempt: int,
    base: float,
    maximum: float,
    retry_after: str | None = None,
    jitter: Callable[[], float] = random.random,
) -> float:
    """Seconds to wait before the next try.

    ``attempt`` counts from 1 (the attempt that just failed). A numeric ``Retry-After``
    header wins; otherwise the delay doubles each attempt (base, 2*base, 4*base, ...),
    plus up to 10% jitter, capped at ``maximum``.
    """
    if retry_after is not None:
        try:
            return min(max(float(retry_after), 0.0), maximum)
        except ValueError:
            pass  # HTTP-date format: fall back to exponential backoff
    delay = base * (2 ** (attempt - 1))
    return min(delay * (1 + 0.1 * jitter()), maximum)


class HttpClient:
    def __init__(
        self,
        settings: HttpSettings,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        self._sleep = sleep

    def get(self, url: str, params: dict[str, Any] | None = None) -> requests.Response:
        """GET ``url``; return the response once it succeeds (2xx/3xx)."""
        max_attempts = self.settings.max_attempts
        for attempt in range(1, max_attempts + 1):
            retry_after: str | None = None
            try:
                response = self.session.get(
                    url, params=params, timeout=self.settings.timeout_seconds
                )
            except RETRYABLE_EXCEPTIONS as exc:
                reason, status = f"{type(exc).__name__}: {exc}", None
            else:
                if response.status_code < 400:
                    return response
                status = response.status_code
                if status not in RETRYABLE_STATUS:
                    raise HttpRequestError(f"GET {url} failed with HTTP {status}", status)
                reason = f"HTTP {status}"
                retry_after = response.headers.get("Retry-After")

            if attempt == max_attempts:
                raise HttpRequestError(
                    f"GET {url} failed after {max_attempts} attempts ({reason})", status
                )
            delay = backoff_delay(
                attempt,
                self.settings.backoff_base_seconds,
                self.settings.backoff_max_seconds,
                retry_after,
            )
            log.warning(
                "request failed, retrying",
                extra={
                    "url": url,
                    "attempt": attempt,
                    "reason": reason,
                    "delay_s": round(delay, 2),
                },
            )
            self._sleep(delay)

        raise AssertionError("unreachable")  # pragma: no cover

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        return self.get(url, params=params).json()
