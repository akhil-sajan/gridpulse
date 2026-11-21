"""Time helpers for German grid data.

All silver and gold keys are UTC instants. German local time (Europe/Berlin) is a
derived attribute, and local days are not all the same length:

- the last Sunday of March has 23 hours (92 quarter-hours): clocks jump 02:00 -> 03:00,
- the last Sunday of October has 25 hours (100 quarter-hours): 02:00-03:00 happens twice,
- every other day has 24 hours (96 quarter-hours).

These helpers state those rules in plain Python so they can be unit-tested; the silver
checks apply the same rules in Spark SQL.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")


def local_day_utc_bounds(day: date, tz: ZoneInfo = BERLIN) -> tuple[datetime, datetime]:
    """UTC start (inclusive) and end (exclusive) of a local calendar day."""
    start = datetime.combine(day, time(0), tzinfo=tz).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=tz).astimezone(UTC)
    return start, end


def intervals_in_local_day(day: date, minutes: int, tz: ZoneInfo = BERLIN) -> int:
    """How many intervals of ``minutes`` a local day contains (DST-aware)."""
    start, end = local_day_utc_bounds(day, tz)
    return int((end - start).total_seconds() // (minutes * 60))


def quarter_hours_in_local_day(day: date, tz: ZoneInfo = BERLIN) -> int:
    return intervals_in_local_day(day, 15, tz)


def hours_in_local_day(day: date, tz: ZoneInfo = BERLIN) -> int:
    return intervals_in_local_day(day, 60, tz)


def dst_change_days(year: int, tz: ZoneInfo = BERLIN) -> list[date]:
    """Local days in ``year`` that are not 24 hours long (the DST switch days)."""
    day, last = date(year, 1, 1), date(year, 12, 31)
    days = []
    while day <= last:
        if hours_in_local_day(day, tz) != 24:
            days.append(day)
        day += timedelta(days=1)
    return days


def quarter_hours_in_utc_year(year: int) -> int:
    """Quarter-hours in a UTC calendar year (35,040, or 35,136 in a leap year)."""
    days = (date(year + 1, 1, 1) - date(year, 1, 1)).days
    return days * 96
