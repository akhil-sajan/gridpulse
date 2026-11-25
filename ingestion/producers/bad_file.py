"""Write a deliberately broken stream file for the quarantine test (P5.1).

It imitates a buggy producer that skipped validation and wrote straight to the landing
volume, so silver has to catch the problems itself:

1. a value that is not JSON at all                 -> event_id_present, ... fail
2. a meter reading with negative energy             -> energy_in_range fails
3. a reading with an unknown schema version         -> schema_version_ok fails

Usage::

    python -m ingestion.producers.bad_file bad_test.jsonl
    databricks fs cp bad_test.jsonl \
        dbfs:/Volumes/gridpulse_dev/bronze/landing/stream/meters_readings_v1/bad_test.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from ingestion.producers.events import event_id, iso_utc

TOPIC = "meters.readings.v1"


def bad_lines(now: datetime | None = None) -> list[str]:
    now = now or datetime.now(UTC)

    def event(version: str, kwh: float, n: int) -> str:
        return json.dumps(
            {
                "event_id": event_id("bad-file-test", n, now.isoformat()),
                "event_ts_utc": "2013-01-05T00:00:00.000Z",
                "produced_at": iso_utc(now),
                "schema_version": version,
                "source": "london-smart-meters",
                "payload": {
                    "meter_id": "MAC999999",
                    "energy_kwh": kwh,
                    "interval_minutes": 30,
                    "replay_id": "bad-file-test",
                },
            }
        )

    values = [
        "this is not json {",
        event("meters.readings.v1", -5.0, 2),
        event("meters.readings.v9", 0.4, 3),
    ]
    return [json.dumps({"topic": TOPIC, "key": "MAC999999", "value": v}) for v in values]


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    out = Path(args[0] if args else "bad_test.jsonl")
    out.write_text("\n".join(bad_lines()) + "\n", encoding="utf-8")
    print(f"Wrote 3 broken messages to {out}")
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
