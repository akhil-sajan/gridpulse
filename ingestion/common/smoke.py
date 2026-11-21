"""P1.1 smoke test: fetch live data and land it in the dev volume.

Run from the repo root::

    python -m ingestion.common.smoke

It fetches the current German generation mix from Energy-Charts with the retrying
HTTP client and writes it to
``/Volumes/gridpulse_dev/bronze/landing/_smoke/energycharts/ingest_date=<today>/...``.
Running it twice in the same minute skips the second upload (idempotency check).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from ingestion.common.config import load_settings
from ingestion.common.http import HttpClient
from ingestion.common.landing import make_writer, upload_to_landing
from ingestion.common.logs import get_logger

ENERGY_CHARTS_URL = "https://api.energy-charts.info/public_power"

log = get_logger("smoke")


def main() -> int:
    settings = load_settings()
    log.info("settings loaded", extra={"env": settings.env, "landing_root": settings.landing_root})

    client = HttpClient(settings.http)
    payload = client.get_json(ENERGY_CHARTS_URL, params={"country": "de"})
    data = json.dumps(payload).encode("utf-8")

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%MZ")
    writer = make_writer(settings)
    result = upload_to_landing(
        writer, settings, "_smoke", "energycharts", f"public_power_{stamp}.json", data
    )
    confirmed = writer.exists(result.path)
    log.info(
        "smoke test finished",
        extra={"path": result.path, "skipped": result.skipped, "confirmed": confirmed},
    )
    print(f"\nSmoke test {'PASSED' if confirmed else 'FAILED'}: {result.path}")
    return 0 if confirmed else 1


if __name__ == "__main__":
    sys.exit(main())
