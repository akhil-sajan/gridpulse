"""Tests for the Open-Meteo ingester. HTTP is faked with Open-Meteo-shaped responses."""

import json
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from ingestion.batch.open_meteo import (
    Breaker,
    Location,
    Month,
    Pacer,
    archive_file_name,
    final_archive_keys,
    is_final,
    load_config,
    months_between,
    plan_archive,
    request_weight,
    run_archive,
    run_forecast,
)
from ingestion.common.config import HttpSettings, load_settings
from ingestion.common.http import HttpClient
from ingestion.common.landing import LocalLandingWriter

HE = Location("de_he", "Hessen", "Wiesbaden", 50.0782, 8.2398)
BY = Location("de_by", "Bayern", "München", 48.1351, 11.582)
TODAY = date(2024, 3, 5)
NOW = datetime(2024, 3, 5, 6, 0, tzinfo=UTC)


@pytest.fixture
def config():
    cfg = load_config()
    return replace(cfg, start_date=date(2024, 1, 1), locations=(HE, BY))


# --------------------------------------------------------------------- config and helpers


def test_repo_config_is_consistent():
    cfg = load_config()
    codes = [loc.code for loc in cfg.locations]
    assert len(codes) == 16 and len(set(codes)) == 16
    assert all(47 < loc.lat < 55.5 and 5.5 < loc.lon < 15.5 for loc in cfg.locations)
    assert "shortwave_radiation" in cfg.variables and "wind_speed_100m" in cfg.variables
    assert cfg.budget_per_minute < 600 and cfg.budget_per_run < 10000


@pytest.mark.parametrize(
    ("days", "variables", "locations", "expected"),
    [(14, 4, 1, 1.0), (7, 4, 1, 1.0), (31, 4, 1, 31 / 14), (28, 20, 1, 4.0), (3, 4, 16, 16.0)],
)
def test_request_weight(days, variables, locations, expected):
    assert request_weight(days, variables, locations) == pytest.approx(expected)


def test_months_between_spans_year_boundary():
    months = months_between(date(2023, 11, 15), date(2024, 2, 1))
    assert [m.label for m in months] == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert months[-1].end == date(2024, 2, 29)  # leap year


def test_month_final_after_grace_period():
    jan = Month(date(2024, 1, 1), date(2024, 1, 31))
    assert not is_final(jan, date(2024, 2, 7), final_after_days=7)
    assert is_final(jan, date(2024, 2, 8), final_after_days=7)


def test_archive_names_round_trip():
    jan = Month(date(2024, 1, 1), date(2024, 1, 31))
    final = archive_file_name(HE, jan)
    partial = archive_file_name(BY, jan, "20240205T0600Z")
    assert final == "de_he_2024-01.json"
    assert partial == "de_by_2024-01_partial_20240205T0600Z.json"
    assert final_archive_keys([f"/a/ingest_date=2024-02-05/{final}", partial]) == {
        ("de_he", "2024-01")
    }


def test_pacer_spaces_requests_by_weight():
    sleeps, clock = [], [0.0]

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    pacer = Pacer(budget_per_minute=60, sleep=sleep, clock=lambda: clock[0])  # 1 call/second
    pacer.wait(2.0)  # first request goes immediately
    pacer.wait(1.0)  # must wait 2 s (weight of the previous request)
    pacer.wait(1.0)  # must wait 1 s
    assert sleeps == [pytest.approx(2.0), pytest.approx(1.0)]


# --------------------------------------------------------------------- planning


def test_plan_caps_current_month_at_yesterday_and_marks_open_months(config):
    tasks = plan_archive(config, [HE], landed=set(), today=TODAY)
    assert [(t.month.label, t.final) for t in tasks] == [
        ("2024-01", True),  # ended > 7 days ago
        ("2024-02", False),  # ended only 5 days ago
        ("2024-03", False),  # current month
    ]
    assert tasks[-1].start == date(2024, 3, 1) and tasks[-1].end == date(2024, 3, 4)


def test_plan_skips_final_months_already_landed(config):
    tasks = plan_archive(config, [HE, BY], landed={("de_he", "2024-01")}, today=TODAY)
    keys = {(t.location.code, t.month.label) for t in tasks}
    assert ("de_he", "2024-01") not in keys
    assert ("de_by", "2024-01") in keys and ("de_he", "2024-02") in keys


def test_plan_starts_mid_month_when_start_date_does(config):
    cfg = replace(config, start_date=date(2024, 1, 20))
    first = plan_archive(cfg, [HE], set(), TODAY)[0]
    assert first.start == date(2024, 1, 20) and first.days == 12


# --------------------------------------------------------------------- end to end


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.headers = {}

    def json(self):
        return self._payload


class FakeOpenMeteo:
    def __init__(self, fail_for=None):
        self.headers = {}
        self.calls = []
        self.fail_for = fail_for or set()

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        key = (params["latitude"], params.get("start_date"))
        if key in self.fail_for:
            return FakeResponse(400, {"error": True, "reason": "test"})
        return FakeResponse(
            200,
            {
                "latitude": params["latitude"],
                "longitude": params["longitude"],
                "hourly": {"time": ["2024-01-01T00:00"], "temperature_2m": [1.5]},
            },
        )


@pytest.fixture
def env(tmp_path, config):
    settings = load_settings(
        environ={"GRIDPULSE_LANDING_BACKEND": "local", "GRIDPULSE_LOCAL_LANDING_DIR": str(tmp_path)}
    )
    fake = FakeOpenMeteo()
    http = HttpClient(HttpSettings(5, 1, 0.0, 0.0, "test"), session=fake, sleep=lambda _: None)
    writer = LocalLandingWriter(tmp_path)
    pacer = Pacer(480, sleep=lambda _: None)
    return settings, config, http, writer, pacer, fake


def test_archive_first_run_lands_all_months(env):
    settings, config, http, writer, pacer, fake = env
    summary = run_archive(settings, config, http, writer, pacer, now=NOW)

    assert summary.ok, summary.errors
    assert summary.counts["landed_final"] == 2  # January for both locations
    assert summary.counts["landed_partial"] == 4  # February and March for both
    files = writer.list_files(f"{settings.landing_root}/open_meteo/archive")
    assert len(files) == 6
    url, params = fake.calls[0]
    assert url == config.archive_url
    assert params["hourly"] == ",".join(config.variables)
    assert params["start_date"] == "2024-01-01" and params["end_date"] == "2024-01-31"
    assert summary.weight_used > 0


def test_archive_second_run_refetches_only_open_months(env):
    settings, config, http, writer, pacer, fake = env
    run_archive(settings, config, http, writer, pacer, now=NOW)
    fake.calls.clear()
    later = datetime(2024, 3, 5, 7, 0, tzinfo=UTC)
    summary = run_archive(settings, config, http, writer, pacer, now=later)
    assert summary.counts["planned"] == 4
    assert "landed_final" not in summary.counts
    assert all(p["start_date"] >= "2024-02-01" for _, p in fake.calls)


def test_archive_respects_run_budget_and_defers_the_rest(env):
    settings, config, http, writer, pacer, _ = env
    small = replace(config, budget_per_run=5.0)  # each month costs about 2.1-2.2
    summary = run_archive(settings, small, http, writer, pacer, now=NOW)
    assert summary.counts["planned"] == 2
    assert summary.counts["deferred"] == 4
    assert summary.weight_used <= 5.0


def test_archive_dry_run_and_max_requests(env):
    settings, config, http, writer, pacer, fake = env
    summary = run_archive(settings, config, http, writer, pacer, now=NOW, dry_run=True)
    assert summary.counts["would_land_final"] == 2
    assert summary.counts["would_land_partial"] == 4
    assert fake.calls == []

    summary = run_archive(settings, config, http, writer, pacer, now=NOW, max_requests=3)
    assert summary.counts["planned"] == 3


def test_archive_errors_are_collected(env):
    settings, config, http, writer, pacer, fake = env
    fake.fail_for = {(HE.lat, "2024-02-01")}
    summary = run_archive(settings, config, http, writer, pacer, now=NOW)
    assert len(summary.errors) == 1 and "de_he 2024-02" in summary.errors[0]
    assert summary.counts["landed_final"] + summary.counts["landed_partial"] == 5


def test_forecast_lands_one_file_per_location(env):
    settings, config, http, writer, pacer, fake = env
    summary = run_forecast(settings, config, http, writer, pacer, now=NOW)
    assert summary.ok and summary.counts["landed"] == 2
    files = writer.list_files(f"{settings.landing_root}/open_meteo/forecast")
    assert sorted(f.rsplit("/", 1)[-1] for f in files) == [
        "de_by_forecast_20240305T0600Z.json",
        "de_he_forecast_20240305T0600Z.json",
    ]
    assert fake.calls[0][1]["forecast_days"] == config.forecast_days
    assert summary.weight_used == pytest.approx(2.0)


def test_location_filter(env):
    settings, config, http, writer, pacer, _ = env
    summary = run_forecast(settings, config, http, writer, pacer, now=NOW, location_codes={"de_he"})
    assert summary.counts["landed"] == 1


class DownOpenMeteo(FakeOpenMeteo):
    """Every request fails, as when the API is unavailable."""

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return FakeResponse(503, {"error": True})


def test_archive_stops_after_consecutive_failures(env, tmp_path):
    settings, config, _, writer, pacer, _ = env
    down = DownOpenMeteo()
    http = HttpClient(HttpSettings(5, 1, 0.0, 0.0, "test"), session=down, sleep=lambda _: None)
    summary = run_archive(settings, config, http, writer, pacer, now=NOW)
    # 6 months planned, stopped after 3 failures in a row: 3 requests, not 6.
    assert len(down.calls) == 3
    assert summary.counts["stopped_early"] == 3
    assert any("stopped early (3 failed requests in a row)" in e for e in summary.errors)


def test_breaker_resets_on_success_and_enforces_time_limit():
    t = [0.0]
    b = Breaker(max_failures=2, max_minutes=1, clock=lambda: t[0])
    b.failure()
    b.success()
    b.failure()
    assert not b.should_stop()  # failures not in a row
    t[0] = 61
    assert b.should_stop() and b.reason == "time limit reached"


def test_forecast_stops_when_api_is_down(env):
    settings, config, _, writer, pacer, _ = env
    down = DownOpenMeteo()
    http = HttpClient(HttpSettings(5, 1, 0.0, 0.0, "test"), session=down, sleep=lambda _: None)
    summary = run_forecast(
        settings, config, http, writer, pacer, now=NOW, breaker=Breaker(max_failures=1)
    )
    assert len(down.calls) == 1 and summary.counts["stopped_early"] == 1
