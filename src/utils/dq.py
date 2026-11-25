"""Pure decision logic for freshness and volume checks (P5.2).

Kept free of Spark so it can be unit-tested; the job notebook measures, this module
decides. Every check returns a ``Check`` that is written to ``ops.data_quality_checks``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

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
    last_24h: int,
    previous_days: list[int],
    min_ratio: float = 0.5,
    min_history_days: int = 3,
) -> Check:
    """Rows of the last 24 h compared with the average of the previous days.

    Fewer than ``min_ratio`` x average fails (a source delivering only partly). With too
    little history, or an average of zero, there is nothing to compare: a warning, not
    a failure, so a new table doesn't page anyone.
    """
    days = [d for d in previous_days if d is not None]
    if len(days) < min_history_days:
        return Check(
            "volume",
            table,
            float(last_24h),
            min_ratio,
            WARN,
            f"only {len(days)} day(s) of history; {last_24h} rows in the last 24 h",
        )
    average = sum(days) / len(days)
    if average == 0:
        return Check("volume", table, float(last_24h), min_ratio, WARN, "7-day average is 0")
    ratio = last_24h / average
    status = PASS if ratio >= min_ratio else FAIL
    detail = (
        f"{last_24h} rows vs 7-day average {average:.0f} ({ratio:.0%}, minimum {min_ratio:.0%})"
    )
    return Check("volume", table, round(ratio, 3), min_ratio, status, detail)


def failures(checks: list[Check]) -> list[Check]:
    return [c for c in checks if c.status == FAIL]
