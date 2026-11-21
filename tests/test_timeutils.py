from datetime import UTC, date, datetime

import pytest

from src.utils.timeutils import (
    dst_change_days,
    hours_in_local_day,
    local_day_utc_bounds,
    quarter_hours_in_local_day,
    quarter_hours_in_utc_year,
)


def test_spring_forward_day_has_92_quarter_hours():
    assert quarter_hours_in_local_day(date(2024, 3, 31)) == 92
    assert hours_in_local_day(date(2024, 3, 31)) == 23


def test_fall_back_day_has_100_quarter_hours():
    assert quarter_hours_in_local_day(date(2024, 10, 27)) == 100
    assert hours_in_local_day(date(2024, 10, 27)) == 25


@pytest.mark.parametrize("day", [date(2024, 1, 1), date(2024, 6, 21), date(2024, 12, 31)])
def test_normal_days_have_96_quarter_hours(day):
    assert quarter_hours_in_local_day(day) == 96


def test_local_day_bounds_in_winter_and_on_switch_days():
    # Winter: local midnight is 23:00 UTC the day before.
    assert local_day_utc_bounds(date(2024, 1, 1)) == (
        datetime(2023, 12, 31, 23, tzinfo=UTC),
        datetime(2024, 1, 1, 23, tzinfo=UTC),
    )
    # Spring forward: the day starts in winter time and ends in summer time.
    assert local_day_utc_bounds(date(2024, 3, 31)) == (
        datetime(2024, 3, 30, 23, tzinfo=UTC),
        datetime(2024, 3, 31, 22, tzinfo=UTC),
    )
    # Fall back: starts in summer time, ends in winter time.
    assert local_day_utc_bounds(date(2024, 10, 27)) == (
        datetime(2024, 10, 26, 22, tzinfo=UTC),
        datetime(2024, 10, 27, 23, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("year", "expected"),
    [
        (2020, [date(2020, 3, 29), date(2020, 10, 25)]),
        (2024, [date(2024, 3, 31), date(2024, 10, 27)]),
        (2026, [date(2026, 3, 29), date(2026, 10, 25)]),
    ],
)
def test_dst_change_days(year, expected):
    assert dst_change_days(year) == expected


def test_quarter_hours_in_utc_year():
    assert quarter_hours_in_utc_year(2023) == 35_040
    assert quarter_hours_in_utc_year(2024) == 35_136
    # A local year still has the same total: the lost and gained hour cancel out.
    days = [date(2024, 1, 1).fromordinal(date(2024, 1, 1).toordinal() + i) for i in range(366)]
    assert sum(quarter_hours_in_local_day(d) for d in days) == 35_136
