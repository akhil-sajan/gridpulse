"""Live producer: German public net generation per production type (P4.1).

Polls the energy-charts API (Fraunhofer ISE, CC BY 4.0) every few minutes and sends one
event per production type and quarter-hour to topic ``energy.generation.v1``. The
message key is the production type, so all events of one type land on one partition
and stay in order.

Each poll asks for the last ``--lookback-hours``: the newest quarter-hours are filled in
over time, and earlier values are sometimes revised. Events already sent in this process
are skipped; anything sent twice anyway (after a restart) carries the same deterministic
``event_id`` and is dropped downstream. A revised value gets a new ID and wins in silver.

Run from the repo root::

    python -m ingestion.producers.energy_charts --once --dry-run   # print, don't send
    python -m ingestion.producers.energy_charts --once             # one poll to Kafka
    python -m ingestion.producers.energy_charts                    # poll every 5 minutes

The API allows about 2 requests per minute; one poll is one request.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

from ingestion.common.config import load_settings
from ingestion.common.http import HttpClient
from ingestion.common.logs import get_logger
from ingestion.producers.events import Publisher, make_event, make_sink

log = get_logger("energy_charts")

TOPIC = "energy.generation.v1"
SOURCE = "energy-charts"
URL = "https://api.energy-charts.info/public_power"

# Series in the response that are not generation by one production type.
NOT_GENERATION = re.compile(r"load|share|cross border", re.IGNORECASE)


def slug(name: str) -> str:
    """'Fossil brown coal / lignite' -> 'fossil_brown_coal_lignite'"""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def to_events(
    response: dict, country: str, now: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> Iterator[tuple[str, dict]]:
    """Turn the compact columnar response into (key, event) pairs; skip missing values."""
    stamps = response.get("unix_seconds") or []
    for series in response.get("production_types") or []:
        name = series.get("name", "")
        if not name or NOT_GENERATION.search(name):
            continue
        type_slug = slug(name)
        for unix, value in zip(stamps, series.get("data") or [], strict=False):
            if value is None:  # not published yet
                continue
            ts = datetime.fromtimestamp(unix, UTC)
            event = make_event(
                TOPIC,
                SOURCE,
                ts,
                {
                    "country": country,
                    "production_type": name,
                    "production_type_slug": type_slug,
                    "power_mw": float(value),
                },
                id_parts=(country, type_slug, unix, value),
                now=now,
            )
            yield type_slug, event


def poll(http: HttpClient, country: str, lookback: timedelta) -> dict:
    end = datetime.now(UTC)
    params = {
        "country": country,
        "start": int((end - lookback).timestamp()),
        "end": int(end.timestamp()),
    }
    return http.get_json(URL, params=params)


def run_once(http, publisher: Publisher, country: str, lookback: timedelta, seen: dict) -> int:
    """One poll; returns how many new events were published."""
    response = poll(http, country, lookback)
    new = 0
    cutoff = datetime.now(UTC) - lookback - timedelta(hours=1)
    for key, event in to_events(response, country):
        if event["event_id"] in seen:
            continue
        publisher.publish(TOPIC, key, event)
        seen[event["event_id"]] = datetime.fromisoformat(event["event_ts_utc"])
        new += 1
    publisher.sink.flush()
    for event_id_, ts in list(seen.items()):  # keep memory bounded
        if ts < cutoff:
            del seen[event_id_]
    return new


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stream German generation to Kafka.")
    parser.add_argument("--country", default="de")
    parser.add_argument("--interval", type=int, default=300, help="seconds between polls")
    parser.add_argument("--lookback-hours", type=float, default=3)
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    parser.add_argument("--max-polls", type=int, help="stop after this many polls")
    parser.add_argument("--dry-run", action="store_true", help="print events, don't send")
    parser.add_argument(
        "--transport",
        choices=["kafka", "file"],
        help="kafka (Redpanda) or file (landing volume); default GRIDPULSE_STREAM_TRANSPORT",
    )
    args = parser.parse_args(argv)
    if args.transport:
        os.environ["GRIDPULSE_STREAM_TRANSPORT"] = args.transport

    http = HttpClient(load_settings().http)
    sink = make_sink(dict(os.environ), dry_run=args.dry_run)
    publisher = Publisher(sink)
    lookback = timedelta(hours=args.lookback_hours)
    seen: dict[str, datetime] = {}
    polls = 0
    try:
        while True:
            new = run_once(http, publisher, args.country, lookback, seen)
            polls += 1
            log.info(
                "poll finished",
                extra={
                    "new_events": new,
                    "sent": publisher.sent,
                    "dead_lettered": publisher.dead_lettered,
                    "polls": polls,
                },
            )
            if args.once or (args.max_polls and polls >= args.max_polls):
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        sink.close()

    if args.dry_run:
        for topic, key, value in sink.messages[-5:]:
            print(topic, key, json.dumps(value, ensure_ascii=False))
    print(f"\nSent {publisher.sent} events, {publisher.dead_lettered} to the DLQ, {polls} poll(s).")
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
