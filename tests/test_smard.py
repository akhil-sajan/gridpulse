"""Tests for the SMARD ingester. HTTP is faked with small responses in SMARD's format."""

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from ingestion.batch.smard import (
    Series,
    SmardConfig,
    data_url,
    file_name,
    final_keys,
    index_url,
    is_final,
    load_config,
    plan_tasks,
    run,
)
from ingestion.common.config import HttpSettings, load_settings
from ingestion.common.http import HttpClient
from ingestion.common.landing import LocalLandingWriter

BASE = "https://www.smard.de/app/chart_data"
SOLAR = Series("generation", "solar", 4068, "DE", ("quarterhour",))
PRICE = Series("prices", "day_ahead", 4169, "DE-LU", ("hour", "quarterhour"))

# Week starts (Monday 00:00 Europe/Berlin = Sunday 23:00 UTC in winter), as SMARD lists them.
W1 = 1703458800000  # 2023-12-24 23:00 UTC
W2 = 1704063600000  # 2023-12-31 23:00 UTC
W3 = 1704668400000  # 2024-01-07 23:00 UTC
NOW = datetime(2024, 1, 12, 12, 0, tzinfo=UTC)  # W3 is still open


def week(ts):
    return datetime.fromtimestamp(ts / 1000, tz=UTC)


# --------------------------------------------------------------------- config and names


def test_repo_config_loads_and_is_consistent():
    cfg = load_config()
    assert cfg.start_date == date(2020, 1, 1)
    names = [(s.dataset, s.name) for s in cfg.series]
    assert len(names) == len(set(names)), "duplicate series"
    assert {s.dataset for s in cfg.series} == {"generation", "consumption", "prices"}
    assert all(s.resolutions for s in cfg.series)


def test_urls_follow_smard_scheme():
    assert index_url(BASE, SOLAR, "quarterhour") == f"{BASE}/4068/DE/index_quarterhour.json"
    assert data_url(BASE, PRICE, "hour", W2) == f"{BASE}/4169/DE-LU/4169_DE-LU_hour_{W2}.json"


def test_file_names_round_trip_and_partial_files_are_not_final():
    final = file_name(SOLAR, "quarterhour", W1)
    partial = file_name(SOLAR, "quarterhour", W3, "20240112T1200Z")
    price = file_name(PRICE, "hour", W2)
    assert final == f"solar_4068_DE_quarterhour_{W1}.json"
    assert partial == f"solar_4068_DE_quarterhour_{W3}_partial_20240112T1200Z.json"
    keys = final_keys([f"/x/ingest_date=2024-01-12/{n}" for n in (final, partial, price)])
    assert keys == {(4068, "DE", "quarterhour", W1), (4169, "DE-LU", "hour", W2)}


def test_multi_word_series_names_parse():
    name = file_name(
        Series("generation", "wind_onshore", 4067, "DE", ("quarterhour",)), "quarterhour", W1
    )
    assert final_keys([name]) == {(4067, "DE", "quarterhour", W1)}


# --------------------------------------------------------------------- planning


def test_week_is_final_only_after_grace_period():
    week_end = week(W2) + timedelta(days=7)
    assert not is_final(W2, week_end + timedelta(days=1), final_after_days=2)
    assert is_final(W2, week_end + timedelta(days=2), final_after_days=2)


def test_plan_skips_landed_final_weeks_and_always_refetches_open_ones():
    landed = {(4068, "DE", "quarterhour", W1)}
    tasks = plan_tasks(SOLAR, "quarterhour", [W1, W2, W3], landed, date(2023, 12, 1), NOW, 2)
    assert [(t.ts, t.final) for t in tasks] == [(W2, True), (W3, False)]


def test_plan_respects_start_date_but_keeps_the_week_containing_it():
    # start 2024-01-03 lies inside the week that starts W2, so W1 is dropped, W2 kept.
    tasks = plan_tasks(SOLAR, "quarterhour", [W1, W2, W3], set(), date(2024, 1, 3), NOW, 2)
    assert [t.ts for t in tasks] == [W2, W3]


# --------------------------------------------------------------------- end to end


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.headers = {}

    def json(self):
        return self._payload


class FakeSmard:
    """Serves SMARD-shaped responses by URL; unknown URLs return 404."""

    def __init__(self, routes):
        self.routes = routes
        self.headers = {}
        self.requested = []

    def get(self, url, params=None, timeout=None):
        self.requested.append(url)
        if url in self.routes:
            return FakeResponse(200, self.routes[url])
        return FakeResponse(404)


def series_payload(ts):
    return {"meta_data": {"version": 1, "created": ts}, "series": [[ts, 1.0], [ts + 900000, None]]}


@pytest.fixture
def env(tmp_path):
    settings = load_settings(
        environ={"GRIDPULSE_LANDING_BACKEND": "local", "GRIDPULSE_LOCAL_LANDING_DIR": str(tmp_path)}
    )
    config = SmardConfig(BASE, date(2023, 12, 1), 2, (SOLAR, PRICE))
    routes = {
        index_url(BASE, SOLAR, "quarterhour"): {"timestamps": [W1, W2, W3]},
        index_url(BASE, PRICE, "hour"): {"timestamps": [W2]},
        # PRICE has no quarterhour index -> 404, which must be skipped, not fail the run.
    }
    for ts in (W1, W2, W3):
        routes[data_url(BASE, SOLAR, "quarterhour", ts)] = series_payload(ts)
    routes[data_url(BASE, PRICE, "hour", W2)] = series_payload(W2)

    fake = FakeSmard(routes)
    http_settings = HttpSettings(5, 2, 0.0, 0.0, "test")

    def http_factory():
        return HttpClient(http_settings, session=fake, sleep=lambda _: None)

    def writer_factory():
        return LocalLandingWriter(tmp_path)

    return settings, config, http_factory, writer_factory, fake, tmp_path


def test_first_run_lands_everything_with_raw_payload(env):
    settings, config, http_factory, writer_factory, _, tmp_path = env
    summary = run(settings, config, http_factory, writer_factory, now=NOW, workers=2)

    assert summary.ok, summary.errors
    assert summary.counts["landed_final"] == 3  # solar W1, W2 + price W2
    assert summary.counts["landed_partial"] == 1  # solar W3 (open week)
    assert summary.counts["index_missing"] == 1  # price quarterhour

    files = writer_factory().list_files(f"{settings.landing_root}/smard")
    assert len(files) == 4
    assert all("/smard/generation/" in f or "/smard/prices/" in f for f in files)
    landed = writer_factory()._local(next(f for f in files if f.endswith(f"_{W1}.json")))
    assert json.loads(landed.read_bytes()) == series_payload(W1)  # raw, unchanged


def test_second_run_only_refetches_open_weeks(env):
    settings, config, http_factory, writer_factory, fake, _ = env
    run(settings, config, http_factory, writer_factory, now=NOW)
    fake.requested.clear()

    later = NOW + timedelta(minutes=5)
    summary = run(settings, config, http_factory, writer_factory, now=later)

    assert summary.ok
    assert summary.counts["planned"] == 1
    assert summary.counts["landed_partial"] == 1
    assert "landed_final" not in summary.counts
    data_requests = [u for u in fake.requested if "index_" not in u]
    assert data_requests == [data_url(BASE, SOLAR, "quarterhour", W3)]


def test_open_week_becomes_final_once_grace_period_passes(env):
    settings, config, http_factory, writer_factory, _, _ = env
    run(settings, config, http_factory, writer_factory, now=NOW)
    much_later = week(W3) + timedelta(days=10)
    summary = run(settings, config, http_factory, writer_factory, now=much_later)
    assert summary.counts["landed_final"] == 1  # W3, now final
    assert summary.counts["planned"] == 1


def test_dry_run_downloads_nothing(env):
    settings, config, http_factory, writer_factory, fake, _ = env
    summary = run(settings, config, http_factory, writer_factory, now=NOW, dry_run=True)
    assert summary.counts["would_land_final"] == 3
    assert summary.counts["would_land_partial"] == 1
    assert all("index_" in u for u in fake.requested)
    assert writer_factory().list_files(f"{settings.landing_root}/smard") == []


def test_max_files_and_dataset_filter(env):
    settings, config, http_factory, writer_factory, _, _ = env
    summary = run(
        settings,
        config,
        http_factory,
        writer_factory,
        now=NOW,
        datasets={"generation"},
        max_files=2,
    )
    assert summary.counts["planned"] == 2
    files = writer_factory().list_files(f"{settings.landing_root}/smard")
    assert len(files) == 2 and all("/generation/" in f for f in files)


def test_download_errors_are_collected_not_raised(env):
    settings, config, http_factory, writer_factory, fake, _ = env
    del fake.routes[data_url(BASE, SOLAR, "quarterhour", W2)]  # will 404
    summary = run(settings, config, http_factory, writer_factory, now=NOW)
    assert not summary.ok
    assert len(summary.errors) == 1 and str(W2) in summary.errors[0]
    assert summary.counts["landed_final"] == 2


def test_main_start_date_overrides_config(monkeypatch):
    """P5.5: prod passes --start-date to backfill only a short window."""
    from collections import Counter
    from datetime import date

    from ingestion.batch import smard

    seen = {}

    def fake_run(settings, config, **kwargs):
        seen["start"] = config.start_date
        return smard.RunSummary(Counter(), [])

    monkeypatch.setattr(smard, "run", fake_run)
    assert smard.main(["--start-date", "2026-11-01", "--dry-run"]) == 0
    assert seen["start"] == date(2026, 11, 1)
    assert smard.main(["--start-date", "", "--dry-run"]) == 0
    assert seen["start"] == smard.load_config().start_date
