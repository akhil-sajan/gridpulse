"""Open-Meteo ingester: hourly weather for one point per German state (P1.3).

Two modes:

``archive``
    Historical hourly weather from ``start_date`` until yesterday, one request per
    location and calendar month. Final months (ended ``final_after_days`` ago) are
    landed once and skipped afterwards; recent months are re-downloaded on every run
    (reanalysis data arrives with a delay) and landed with a ``_partial_<fetch time>``
    suffix. Silver keeps the latest value per location and hour.

``forecast``
    The next ~48 hours for every location, landed with the fetch time in the name.

Open-Meteo weighs requests: one covering more than 14 days or 10 variables counts as
several calls. The ingester computes each request's weight, paces requests to stay
under ``budget_per_minute`` and stops planning once ``budget_per_run`` is reached;
the next run continues where this one stopped.

Landing layout::

    /Volumes/<catalog>/bronze/landing/open_meteo/archive/ingest_date=.../de_bw_2024-01.json
    /Volumes/<catalog>/bronze/landing/open_meteo/forecast/ingest_date=.../de_bw_forecast_<ts>.json

Run from the repo root::

    python -m ingestion.batch.open_meteo archive --dry-run
    python -m ingestion.batch.open_meteo archive
    python -m ingestion.batch.open_meteo forecast
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml

from ingestion.common.config import Settings, load_settings, start_date_arg
from ingestion.common.http import HttpClient
from ingestion.common.landing import LandingWriter, make_writer, upload_to_landing
from ingestion.common.logs import get_logger

SOURCE = "open_meteo"
DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "open_meteo.yaml"
ARCHIVE_RE = re.compile(
    r"^(?P<code>[a-z_]+?)_(?P<month>\d{4}-\d{2})(?P<partial>_partial_\d{8}T\d{4}Z)?\.json$"
)

log = get_logger("open_meteo")


# --------------------------------------------------------------------------- config


@dataclass(frozen=True)
class Location:
    code: str
    state: str
    city: str
    lat: float
    lon: float


@dataclass(frozen=True)
class OpenMeteoConfig:
    archive_url: str
    forecast_url: str
    start_date: date
    final_after_days: int
    variables: tuple[str, ...]
    forecast_days: int
    budget_per_minute: float
    budget_per_run: float
    locations: tuple[Location, ...]


def load_config(path: str | Path = DEFAULT_CONFIG) -> OpenMeteoConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return OpenMeteoConfig(
        archive_url=raw["archive_url"],
        forecast_url=raw["forecast_url"],
        start_date=date.fromisoformat(str(raw["start_date"])),
        final_after_days=int(raw["final_after_days"]),
        variables=tuple(raw["variables"]),
        forecast_days=int(raw["forecast_days"]),
        budget_per_minute=float(raw["budget_per_minute"]),
        budget_per_run=float(raw["budget_per_run"]),
        locations=tuple(
            Location(
                code=loc["code"],
                state=loc["state"],
                city=loc["city"],
                lat=float(loc["lat"]),
                lon=float(loc["lon"]),
            )
            for loc in raw["locations"]
        ),
    )


# --------------------------------------------------------------------------- rate limits


def request_weight(days: int, n_variables: int, n_locations: int = 1) -> float:
    """Open-Meteo's call weight: more than 14 days or 10 variables count extra."""
    return n_locations * max(1.0, days / 14) * max(1.0, n_variables / 10)


class Pacer:
    """Spaces requests so the weighted call rate stays under ``budget_per_minute``."""

    def __init__(
        self,
        budget_per_minute: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.seconds_per_call = 60.0 / budget_per_minute
        self._sleep = sleep
        self._clock = clock
        self._next_allowed = 0.0

    def wait(self, weight: float) -> None:
        now = self._clock()
        if now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + weight * self.seconds_per_call


# --------------------------------------------------------------------------- planning


@dataclass(frozen=True)
class Month:
    start: date
    end: date  # last day of the calendar month

    @property
    def label(self) -> str:
        return self.start.strftime("%Y-%m")


def months_between(start: date, last_day: date) -> list[Month]:
    """Calendar months from the month of ``start`` up to the month of ``last_day``."""
    months = []
    year, month = start.year, start.month
    while (year, month) <= (last_day.year, last_day.month):
        days = calendar.monthrange(year, month)[1]
        months.append(Month(date(year, month, 1), date(year, month, days)))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def is_final(month: Month, today: date, final_after_days: int) -> bool:
    return month.end + timedelta(days=final_after_days) < today


def archive_file_name(loc: Location, month: Month, partial_stamp: str | None = None) -> str:
    base = f"{loc.code}_{month.label}"
    return f"{base}_partial_{partial_stamp}.json" if partial_stamp else f"{base}.json"


def final_archive_keys(paths: Iterable[str]) -> set[tuple[str, str]]:
    """(location code, YYYY-MM) of months already landed as final files."""
    keys = set()
    for path in paths:
        match = ARCHIVE_RE.match(path.rsplit("/", 1)[-1])
        if match and not match["partial"]:
            keys.add((match["code"], match["month"]))
    return keys


@dataclass(frozen=True)
class ArchiveTask:
    location: Location
    month: Month
    start: date
    end: date  # capped at yesterday for the current month
    final: bool

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def plan_archive(
    config: OpenMeteoConfig,
    locations: Iterable[Location],
    landed: set[tuple[str, str]],
    today: date,
) -> list[ArchiveTask]:
    yesterday = today - timedelta(days=1)
    tasks = []
    for month in months_between(config.start_date, yesterday):
        start = max(month.start, config.start_date)
        end = min(month.end, yesterday)
        final = is_final(month, today, config.final_after_days)
        for loc in locations:
            if final and (loc.code, month.label) in landed:
                continue
            tasks.append(ArchiveTask(loc, month, start, end, final))
    return tasks


def archive_params(config: OpenMeteoConfig, task: ArchiveTask) -> dict[str, str | float]:
    return {
        "latitude": task.location.lat,
        "longitude": task.location.lon,
        "start_date": task.start.isoformat(),
        "end_date": task.end.isoformat(),
        "hourly": ",".join(config.variables),
    }


def forecast_params(config: OpenMeteoConfig, loc: Location) -> dict[str, str | float | int]:
    return {
        "latitude": loc.lat,
        "longitude": loc.lon,
        "hourly": ",".join(config.variables),
        "forecast_days": config.forecast_days,
    }


# --------------------------------------------------------------------------- running


@dataclass
class RunSummary:
    counts: Counter
    errors: list[str]
    weight_used: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.errors


MAX_CONSECUTIVE_FAILURES = 3
DEFAULT_MAX_MINUTES = 15.0


class Breaker:
    """Stops a run early when the API is clearly unavailable or the run takes too long.

    Every planned request is idempotent (missing months are planned again next run), so
    stopping early only delays data; it never loses any. Without this, a down API cost
    up to 5 attempts x (30 s timeout + 60 s backoff) per request, and 16 requests ran
    into the job's 60-minute timeout.
    """

    def __init__(
        self,
        max_failures: int = MAX_CONSECUTIVE_FAILURES,
        max_minutes: float = DEFAULT_MAX_MINUTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_failures = max_failures
        self.deadline = clock() + max_minutes * 60
        self.clock = clock
        self.failures_in_a_row = 0
        self.reason: str | None = None

    def success(self) -> None:
        self.failures_in_a_row = 0

    def failure(self) -> None:
        self.failures_in_a_row += 1
        if self.failures_in_a_row >= self.max_failures:
            self.reason = f"{self.failures_in_a_row} failed requests in a row"

    def should_stop(self) -> bool:
        if self.reason is None and self.clock() >= self.deadline:
            self.reason = "time limit reached"
        return self.reason is not None


def run_archive(
    settings: Settings,
    config: OpenMeteoConfig,
    http: HttpClient,
    writer: LandingWriter,
    pacer: Pacer,
    today: date | None = None,
    now: datetime | None = None,
    location_codes: set[str] | None = None,
    max_requests: int | None = None,
    dry_run: bool = False,
    breaker: Breaker | None = None,
) -> RunSummary:
    breaker = breaker or Breaker()
    now = now or datetime.now(UTC)
    today = today or now.date()
    stamp = now.strftime("%Y%m%dT%H%MZ")
    summary = RunSummary(Counter(), [])

    locations = [
        loc for loc in config.locations if location_codes is None or loc.code in location_codes
    ]
    landed = final_archive_keys(writer.list_files(f"{settings.landing_root}/{SOURCE}/archive"))
    tasks = plan_archive(config, locations, landed, today)

    # Keep within the per-run budget; whatever doesn't fit is picked up next run.
    selected, budget = [], config.budget_per_run
    for task in tasks:
        weight = request_weight(task.days, len(config.variables))
        if budget - weight < 0 or (max_requests is not None and len(selected) >= max_requests):
            break
        selected.append((task, weight))
        budget -= weight
    summary.counts["planned"] = len(selected)
    summary.counts["deferred"] = len(tasks) - len(selected)
    planned_weight = sum(w for _, w in selected)
    log.info(
        "archive plan ready",
        extra={
            "planned": len(selected),
            "deferred": summary.counts["deferred"],
            "weight": round(planned_weight, 1),
            "dry_run": dry_run,
        },
    )
    if dry_run:
        for task, _ in selected:
            summary.counts[f"would_land_{'final' if task.final else 'partial'}"] += 1
        summary.weight_used = planned_weight
        return summary

    for i, (task, weight) in enumerate(selected, start=1):
        if breaker.should_stop():
            summary.counts["stopped_early"] = len(selected) - i + 1
            summary.errors.append(f"stopped early ({breaker.reason}); rest follows next run")
            break
        name = archive_file_name(task.location, task.month, None if task.final else stamp)
        try:
            pacer.wait(weight)
            response = http.get(config.archive_url, params=archive_params(config, task))
            summary.weight_used += weight
            result = upload_to_landing(writer, settings, SOURCE, "archive", name, response.content)
        except Exception as exc:  # noqa: BLE001 - collect and report every failure
            summary.errors.append(f"{task.location.code} {task.month.label}: {exc}")
            breaker.failure()
            continue
        breaker.success()
        summary.counts[
            "skipped" if result.skipped else ("landed_final" if task.final else "landed_partial")
        ] += 1
        if i % 100 == 0:
            log.info("progress", extra={"done": i, "total": len(selected)})
    return summary


def run_forecast(
    settings: Settings,
    config: OpenMeteoConfig,
    http: HttpClient,
    writer: LandingWriter,
    pacer: Pacer,
    now: datetime | None = None,
    location_codes: set[str] | None = None,
    dry_run: bool = False,
    breaker: Breaker | None = None,
) -> RunSummary:
    breaker = breaker or Breaker()
    now = now or datetime.now(UTC)
    stamp = now.strftime("%Y%m%dT%H%MZ")
    summary = RunSummary(Counter(), [])
    locations = [
        loc for loc in config.locations if location_codes is None or loc.code in location_codes
    ]
    weight = request_weight(config.forecast_days, len(config.variables))
    summary.counts["planned"] = len(locations)
    if dry_run:
        summary.counts["would_land"] = len(locations)
        summary.weight_used = weight * len(locations)
        return summary

    for n, loc in enumerate(locations):
        if breaker.should_stop():
            summary.counts["stopped_early"] = len(locations) - n
            summary.errors.append(f"stopped early ({breaker.reason}); rest follows next run")
            break
        try:
            pacer.wait(weight)
            response = http.get(config.forecast_url, params=forecast_params(config, loc))
            summary.weight_used += weight
            upload_to_landing(
                writer,
                settings,
                SOURCE,
                "forecast",
                f"{loc.code}_forecast_{stamp}.json",
                response.content,
            )
        except Exception as exc:  # noqa: BLE001
            summary.errors.append(f"{loc.code} forecast: {exc}")
            breaker.failure()
            continue
        breaker.success()
        summary.counts["landed"] += 1
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land Open-Meteo weather in the landing zone.")
    parser.add_argument("mode", choices=["archive", "forecast"])
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--start-date",
        type=start_date_arg,
        default=None,
        help="land data from this date on instead of the config start_date (empty = config)",
    )
    parser.add_argument("--location", action="append", help="location code, e.g. de_he")
    parser.add_argument("--max-requests", type=int, help="archive: at most this many requests")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--max-minutes",
        type=float,
        default=DEFAULT_MAX_MINUTES,
        help=f"stop starting new requests after this long (default {DEFAULT_MAX_MINUTES:g})",
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    config = load_config(args.config)
    if args.start_date:
        config = replace(config, start_date=args.start_date)
    http = HttpClient(settings.http)
    writer = make_writer(settings)
    pacer = Pacer(config.budget_per_minute)
    codes = set(args.location) if args.location else None
    log.info("starting", extra={"mode": args.mode, "env": settings.env})

    if args.mode == "archive":
        summary = run_archive(
            settings,
            config,
            http,
            writer,
            pacer,
            location_codes=codes,
            max_requests=args.max_requests,
            dry_run=args.dry_run,
            breaker=Breaker(max_minutes=args.max_minutes),
        )
    else:
        summary = run_forecast(
            settings,
            config,
            http,
            writer,
            pacer,
            location_codes=codes,
            dry_run=args.dry_run,
            breaker=Breaker(max_minutes=args.max_minutes),
        )

    log.info(
        "finished",
        extra={
            "counts": dict(summary.counts),
            "weight_used": round(summary.weight_used, 1),
            "errors": len(summary.errors),
        },
    )
    for err in summary.errors[:20]:
        log.error("failed", extra={"detail": err})

    print(f"\nOpen-Meteo {args.mode} summary:")
    for key, value in sorted(summary.counts.items()):
        print(f"  {key:22} {value}")
    print(f"  {'api_weight_used':22} {summary.weight_used:.1f}")
    print(f"  {'errors':22} {len(summary.errors)}")
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
