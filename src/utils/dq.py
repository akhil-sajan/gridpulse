"""Pure decision logic for freshness and volume checks (P5.2).

Kept free of Spark so it can be unit-tested; the job notebook measures, this module
decides. Every check returns a ``Check`` that is written to ``ops.data_quality_checks``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median

PASS, WARN, FAIL = "pass", "warn", "fail"


@dataclass(frozen=True)
class Check:
    check_type: str  # freshness | volume
    table_name: str
    measured: float | None
    threshold: float
    status: str
    detail: str

    def as_row(self) -> dict:
        return asdict(self)


def freshness(table: str, age_hours: float | None, max_hours: float) -> Check:
    """Newest data must be younger than ``max_hours``. No data at all is a failure."""
    if age_hours is None:
        return Check("freshness", table, None, max_hours, FAIL, "table has no rows")
    status = PASS if age_hours <= max_hours else FAIL
    detail = f"newest data {age_hours:.1f} h old (limit {max_hours:g} h)"
    return Check("freshness", table, round(age_hours, 2), max_hours, status, detail)


def volume(
    table: str,
    day_rows: int,
    previous_days: list[int],
    min_ratio: float = 0.5,
    min_history_days: int = 3,
    day: str = "last day",
) -> Check:
    """Rows of one complete *data* day compared with the median of the days before it.

    Counting by data day (the day the values describe), not by load time, means a large
    backfill doesn't distort the baseline: a backfill loads many days at once, but each
    day still has the same number of rows. The median ignores a single odd day.

    Fewer than ``min_ratio`` x median fails (a source delivering only partly). With too
    little history, or a median of zero, there is nothing to compare: a warning, not a
    failure, so a new table doesn't page anyone.
    """
    days = [d for d in previous_days if d is not None]
    if len(days) < min_history_days:
        return Check(
            "volume",
            table,
            float(day_rows),
            min_ratio,
            WARN,
            f"only {len(days)} day(s) of history; {day_rows} rows on {day}",
        )
    baseline = median(days)
    if baseline == 0:
        return Check("volume", table, float(day_rows), min_ratio, WARN, "7-day median is 0")
    ratio = day_rows / baseline
    status = PASS if ratio >= min_ratio else FAIL
    detail = (
        f"{day_rows} rows on {day} vs 7-day median {baseline:.0f} "
        f"({ratio:.0%}, minimum {min_ratio:.0%})"
    )
    return Check("volume", table, round(ratio, 3), min_ratio, status, detail)


def failures(checks: list[Check]) -> list[Check]:
    return [c for c in checks if c.status == FAIL]
