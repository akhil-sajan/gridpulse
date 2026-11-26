"""SMARD ingester: German generation, consumption and day-ahead prices (P1.2).

SMARD publishes each series as weekly JSON files. For every configured series and
resolution the ingester:

1. downloads the index of weekly file timestamps,
2. keeps weeks from ``start_date`` on,
3. skips weeks that are already final and landed,
4. downloads the rest and lands the raw JSON unchanged.

Finished weeks are landed once as ``<name>_<filter>_<region>_<resolution>_<ts>.json``.
Open weeks (the current week and the last ``final_after_days``) are still being filled
or revised by SMARD, so they are re-downloaded on every run and landed with a
``_partial_<fetch time>`` suffix. Silver keeps the latest version of each timestamp.

Landing layout::

    /Volumes/<catalog>/bronze/landing/smard/<dataset>/ingest_date=YYYY-MM-DD/<file>

Run from the repo root::

    python -m ingestion.batch.smard                  # full backfill / incremental run
    python -m ingestion.batch.smard --dry-run        # show what would be downloaded
    python -m ingestion.batch.smard --max-files 20   # quick test
"""

from __future__ import annotations

import argparse
import re
import sys
import threading
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml

from ingestion.common.config import Settings, load_settings, start_date_arg
from ingestion.common.http import HttpClient, HttpRequestError
from ingestion.common.landing import LandingWriter, make_writer, upload_to_landing
from ingestion.common.logs import get_logger

SOURCE = "smard"
DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "smard.yaml"
WEEK = timedelta(days=7)

FILENAME_RE = re.compile(
    r"^(?P<name>[a-z_]+?)_(?P<filter>\d+)_(?P<region>[A-Za-z-]+)_(?P<resolution>[a-z]+)"
    r"_(?P<ts>\d+)(?P<partial>_partial_\d{8}T\d{4}Z)?\.json$"
)

log = get_logger("smard")


# --------------------------------------------------------------------------- config


@dataclass(frozen=True)
class Series:
    dataset: str
    name: str
    filter_id: int
    region: str
    resolutions: tuple[str, ...]


@dataclass(frozen=True)
class SmardConfig:
    base_url: str
    start_date: date
    final_after_days: int
    series: tuple[Series, ...]


def load_config(path: str | Path = DEFAULT_CONFIG) -> SmardConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    series = tuple(
        Series(
            dataset=s["dataset"],
            name=s["name"],
            filter_id=int(s["filter"]),
            region=s["region"],
            resolutions=tuple(s["resolutions"]),
        )
        for s in raw["series"]
    )
    return SmardConfig(
        base_url=raw["base_url"].rstrip("/"),
        start_date=date.fromisoformat(str(raw["start_date"])),
        final_after_days=int(raw["final_after_days"]),
        series=series,
    )


# --------------------------------------------------------------------------- naming


def index_url(base_url: str, s: Series, resolution: str) -> str:
    return f"{base_url}/{s.filter_id}/{s.region}/index_{resolution}.json"


def data_url(base_url: str, s: Series, resolution: str, ts: int) -> str:
    return f"{base_url}/{s.filter_id}/{s.region}/{s.filter_id}_{s.region}_{resolution}_{ts}.json"


def file_name(s: Series, resolution: str, ts: int, partial_stamp: str | None = None) -> str:
    base = f"{s.name}_{s.filter_id}_{s.region}_{resolution}_{ts}"
    return f"{base}_partial_{partial_stamp}.json" if partial_stamp else f"{base}.json"


Key = tuple[int, str, str, int]  # (filter, region, resolution, week timestamp)


def final_keys(paths: Iterable[str]) -> set[Key]:
    """Weeks already landed as final files, parsed from landed file paths."""
    keys: set[Key] = set()
    for path in paths:
        match = FILENAME_RE.match(path.rsplit("/", 1)[-1])
        if match and not match["partial"]:
            keys.add((int(match["filter"]), match["region"], match["resolution"], int(match["ts"])))
    return keys


# --------------------------------------------------------------------------- planning


def to_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)


def is_final(ts_ms: int, now: datetime, final_after_days: int) -> bool:
    """True when the week starting at ``ts_ms`` ended more than ``final_after_days`` ago."""
    week_start = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    return week_start + WEEK + timedelta(days=final_after_days) <= now


@dataclass(frozen=True)
class Task:
    series: Series
    resolution: str
    ts: int
    final: bool


def plan_tasks(
    s: Series,
    resolution: str,
    timestamps: Iterable[int],
    landed: set[Key],
    start: date,
    now: datetime,
    final_after_days: int,
) -> list[Task]:
    """Which weekly files to download for one series and resolution."""
    # Include the week that contains ``start`` (week files start up to 7 days earlier).
    earliest = to_ms(start) - int(WEEK.total_seconds() * 1000)
    tasks = []
    for ts in sorted(set(timestamps)):
        if ts < earliest:
            continue
        final = is_final(ts, now, final_after_days)
        if final and (s.filter_id, s.region, resolution, ts) in landed:
            continue
        tasks.append(Task(s, resolution, ts, final))
    return tasks


# --------------------------------------------------------------------------- running


@dataclass
class RunSummary:
    counts: Counter
    errors: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def run(
    settings: Settings,
    config: SmardConfig,
    http_factory: Callable[[], HttpClient],
    writer_factory: Callable[[], LandingWriter],
    now: datetime | None = None,
    datasets: set[str] | None = None,
    max_files: int | None = None,
    dry_run: bool = False,
    workers: int = 4,
) -> RunSummary:
    now = now or datetime.now(UTC)
    stamp = now.strftime("%Y%m%dT%H%MZ")
    counts: Counter = Counter()
    errors: list[str] = []

    http = http_factory()
    writer = writer_factory()
    selected = [s for s in config.series if datasets is None or s.dataset in datasets]

    # 1. Plan: read each index and compare with what is already landed.
    tasks: list[Task] = []
    landed_by_dataset: dict[str, set[Key]] = {}
    for s in selected:
        if s.dataset not in landed_by_dataset:
            directory = f"{settings.landing_root}/{SOURCE}/{s.dataset}"
            landed_by_dataset[s.dataset] = final_keys(writer.list_files(directory))
        for resolution in s.resolutions:
            url = index_url(config.base_url, s, resolution)
            try:
                timestamps = http.get_json(url)["timestamps"]
            except HttpRequestError as exc:
                if exc.status_code == 404:
                    log.warning("no index for series", extra={"series": s.name, "res": resolution})
                    counts["index_missing"] += 1
                    continue
                errors.append(f"index {s.name}/{resolution}: {exc}")
                continue
            planned = plan_tasks(
                s,
                resolution,
                timestamps,
                landed_by_dataset[s.dataset],
                config.start_date,
                now,
                config.final_after_days,
            )
            counts["weeks_in_index"] += len(timestamps)
            tasks.extend(planned)

    if max_files is not None:
        tasks = tasks[:max_files]
    counts["planned"] = len(tasks)
    log.info("plan ready", extra={"planned": len(tasks), "dry_run": dry_run})
    if dry_run:
        for t in tasks:
            counts[f"would_land_{'final' if t.final else 'partial'}"] += 1
        return RunSummary(counts, errors)

    # 2. Download and land, a few files in parallel (one HTTP client and writer per thread).
    local = threading.local()

    def worker(task: Task) -> str:
        if not hasattr(local, "http"):
            local.http = http_factory()
            local.writer = writer_factory()
        response = local.http.get(data_url(config.base_url, task.series, task.resolution, task.ts))
        name = file_name(task.series, task.resolution, task.ts, None if task.final else stamp)
        result = upload_to_landing(
            local.writer, settings, SOURCE, task.series.dataset, name, response.content
        )
        if result.skipped:
            return "skipped"
        return "landed_final" if task.final else "landed_partial"

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(worker, t): t for t in tasks}
        for i, future in enumerate(as_completed(futures), start=1):
            task = futures[future]
            try:
                counts[future.result()] += 1
            except Exception as exc:  # noqa: BLE001 - collect and report every failure
                errors.append(f"{task.series.name}/{task.resolution}/{task.ts}: {exc}")
            if i % 100 == 0:
                log.info("progress", extra={"done": i, "total": len(tasks)})

    return RunSummary(counts, errors)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land SMARD weekly files in the landing zone.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--start-date",
        type=start_date_arg,
        default=None,
        help="land data from this date on instead of the config start_date (empty = config)",
    )
    parser.add_argument("--dataset", action="append", help="limit to a dataset (repeatable)")
    parser.add_argument("--max-files", type=int, help="download at most this many files")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    settings = load_settings()
    config = load_config(args.config)
    if args.start_date:
        config = replace(config, start_date=args.start_date)
    log.info("starting", extra={"env": settings.env, "landing_root": settings.landing_root})

    summary = run(
        settings,
        config,
        http_factory=lambda: HttpClient(settings.http),
        writer_factory=lambda: make_writer(settings),
        datasets=set(args.dataset) if args.dataset else None,
        max_files=args.max_files,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    log.info("finished", extra={"counts": dict(summary.counts), "errors": len(summary.errors)})
    for err in summary.errors[:20]:
        log.error("failed", extra={"detail": err})

    print("\nSMARD run summary:")
    for key, value in sorted(summary.counts.items()):
        print(f"  {key:22} {value}")
    print(f"  {'errors':22} {len(summary.errors)}")
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
