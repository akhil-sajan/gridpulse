import pytest
import requests

from ingestion.common.config import HttpSettings
from ingestion.common.http import HttpClient, HttpRequestError, backoff_delay

SETTINGS = HttpSettings(
    timeout_seconds=5,
    max_attempts=4,
    backoff_base_seconds=1.0,
    backoff_max_seconds=30.0,
    user_agent="test-agent",
)


class FakeResponse:
    def __init__(self, status_code, json_data=None, headers=None):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}

    def json(self):
        return self._json


class FakeSession:
    """Returns (or raises) the scripted outcomes in order and records every call."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.headers = {}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_client(outcomes):
    sleeps = []
    session = FakeSession(outcomes)
    client = HttpClient(SETTINGS, session=session, sleep=sleeps.append)
    return client, session, sleeps


def test_success_first_try_sends_user_agent_and_timeout():
    client, session, sleeps = make_client([FakeResponse(200, {"ok": True})])
    assert client.get_json("https://x", params={"a": 1}) == {"ok": True}
    assert session.headers["User-Agent"] == "test-agent"
    assert session.calls == [{"url": "https://x", "params": {"a": 1}, "timeout": 5}]
    assert sleeps == []


def test_retries_5xx_then_succeeds():
    client, session, sleeps = make_client(
        [FakeResponse(503), FakeResponse(502), FakeResponse(200, {"ok": True})]
    )
    assert client.get_json("https://x") == {"ok": True}
    assert len(session.calls) == 3
    assert len(sleeps) == 2
    assert sleeps[1] > sleeps[0]  # backoff grows


def test_retries_network_errors():
    client, session, sleeps = make_client(
        [requests.ConnectionError("boom"), requests.Timeout("slow"), FakeResponse(200, [])]
    )
    assert client.get_json("https://x") == []
    assert len(sleeps) == 2


def test_honors_retry_after_header_on_429():
    client, _, sleeps = make_client(
        [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, {})]
    )
    client.get("https://x")
    assert sleeps == [7.0]


def test_gives_up_after_max_attempts():
    client, session, sleeps = make_client([FakeResponse(500)] * 4)
    with pytest.raises(HttpRequestError, match="after 4 attempts") as err:
        client.get("https://x")
    assert err.value.status_code == 500
    assert len(session.calls) == 4
    assert len(sleeps) == 3  # no sleep after the last attempt


def test_does_not_retry_client_errors():
    client, session, sleeps = make_client([FakeResponse(404)])
    with pytest.raises(HttpRequestError, match="HTTP 404") as err:
        client.get("https://x")
    assert err.value.status_code == 404
    assert len(session.calls) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (10, 30.0)],
)
def test_backoff_doubles_and_is_capped(attempt, expected):
    assert backoff_delay(attempt, base=1.0, maximum=30.0, jitter=lambda: 0.0) == expected


def test_backoff_jitter_adds_at_most_ten_percent():
    assert backoff_delay(3, base=1.0, maximum=30.0, jitter=lambda: 1.0) == pytest.approx(4.4)


def test_retry_after_is_capped_and_bad_values_fall_back():
    assert backoff_delay(1, 1.0, 30.0, retry_after="120") == 30.0
    assert (
        backoff_delay(1, 1.0, 30.0, retry_after="Wed, 21 Oct 2026 07:28:00 GMT", jitter=lambda: 0.0)
        == 1.0
    )
