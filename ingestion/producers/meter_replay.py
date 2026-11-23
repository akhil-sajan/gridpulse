"""Replay producer: London household smart-meter readings as a live stream (P4.1).

The "Smart meters in London" dataset (UK Power Networks, Low Carbon London trial,
2011-2014, via Kaggle) has one reading per household and half-hour. This producer reads a
block file (``block_<n>.csv`` from ``halfhourly_dataset``), takes a window of days, and
sends the readings in time order to topic ``meters.readings.v1``, faster than real time:
``--speed 1440`` replays one day per minute.

* ``event_ts_utc`` is the original reading time (start of the half-hour). The data is
  from 2011-2014; it is not shifted to today. Latency is measured on ``produced_at``.
* The message key is the meter ID, so one meter's readings stay in order on one partition.
* ``event_id`` depends only on meter, time and value, not on the replay run: replaying
  the same hours again produces the same IDs, which the streaming layer drops (P4.5).
  ``replay_id`` in the payload tells the runs apart.

Run from the repo root::

    python -m ingestion.producers.meter_replay --file block_0.csv --days 1 --dry-run
    python -m ingestion.producers.meter_replay --file block_0.csv --start 2013-01-01 \\
        --days 2 --speed 1440 --max-meters 50
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ingestion.common.logs import get_logger
from ingestion.producers.events import KafkaSink, MemorySink, Publisher, make_event

log = get_logger("meter_replay")

TOPIC = "meters.readings.v1"
SOURCE = "london-smart-meters"
SLOT = timedelta(minutes=30)


def parse_ts(value: str) -> datetime:
    """'2013-01-01 00:30:00.0000000' -> aware UTC datetime."""
    return datetime.strptime(value.strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def read_window(
    rows: Iterable[dict[str, str]], start: date | None, days: int, max_meters: int | None
) -> tuple[dict[datetime, list[tuple[str, float]]], date | None]:
    """Readings of the window grouped by half-hour: {slot: [(meter_id, kWh), ...]}.

    Without ``start``, the window begins on the first full day in the file. Rows without
    a numeric value ("Null" in the dataset) are skipped.
    """
    slots: dict[datetime, list[tuple[str, float]]] = defaultdict(list)
    meters: set[str] = set()
    window_start = datetime.combine(start, datetime.min.time(), UTC) if start else None
    for row in rows:
        meter = (row.get("LCLid") or "").strip()
        raw = (row.get("energy(kWh/hh)") or row.get("energy") or "").strip()
        try:
            kwh = float(raw)
        except ValueError:
            continue
        ts = parse_ts(row["tstp"])
        if window_start is None:
            window_start = datetime.combine(ts.date() + timedelta(days=1), datetime.min.time(), UTC)
        if not (window_start <= ts < window_start + timedelta(days=days)):
            continue
        if meter not in meters:
            if max_meters is not None and len(meters) >= max_meters:
                continue
            meters.add(meter)
        slots[ts].append((meter, kwh))
    return dict(slots), window_start.date() if window_start else None


def replay(
    slots: dict[datetime, list[tuple[str, float]]],
    publisher: Publisher,
    replay_id: str,
    speed: float,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Send slot after slot; wait 30 min / speed between slots (speed 0 = no waiting)."""
    sent_slots = 0
    for ts in sorted(slots):
        for meter, kwh in slots[ts]:
            event = make_event(
                TOPIC,
                SOURCE,
                ts,
                {
                    "meter_id": meter,
                    "energy_kwh": kwh,
                    "interval_minutes": 30,
                    "replay_id": replay_id,
                },
                id_parts=(meter, int(ts.timestamp()), kwh),
            )
            publisher.publish(TOPIC, meter, event)
        publisher.sink.flush()
        sent_slots += 1
        if sent_slots % 48 == 0:
            log.info("replayed a day", extra={"up_to": ts.isoformat(), "sent": publisher.sent})
        if speed > 0:
            sleep(SLOT.total_seconds() / speed)
    return sent_slots


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay London smart-meter readings to Kafka.")
    parser.add_argument("--file", required=True, type=Path, help="block_<n>.csv")
    parser.add_argument(
        "--start", type=date.fromisoformat, help="first day (default: first full day)"
    )
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--speed", type=float, default=1440, help="1440 = one day per minute")
    parser.add_argument("--max-meters", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true", help="print events, don't send")
    args = parser.parse_args(argv)
    if not args.file.is_file():
        parser.error(f"file not found: {args.file}")

    with args.file.open(encoding="utf-8", newline="") as fh:
        slots, first_day = read_window(csv.DictReader(fh), args.start, args.days, args.max_meters)
    readings = sum(len(v) for v in slots.values())
    log.info(
        "window loaded",
        extra={
            "first_day": str(first_day),
            "days": args.days,
            "half_hours": len(slots),
            "readings": readings,
        },
    )

    replay_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    sink = MemorySink() if args.dry_run else KafkaSink(dict(os.environ))
    publisher = Publisher(sink)
    try:
        replay(slots, publisher, replay_id, 0 if args.dry_run else args.speed)
    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        sink.close()

    if args.dry_run:
        for topic, key, value in sink.messages[:3]:
            print(topic, key, json.dumps(value))
    print(
        f"\nReplay {replay_id}: {publisher.sent} readings sent, "
        f"{publisher.dead_lettered} to the DLQ, from {first_day} for {args.days} day(s)."
    )
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
