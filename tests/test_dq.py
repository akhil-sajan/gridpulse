"""Unit tests for the freshness and volume decision logic (P5.2)."""

from src.utils import dq


def test_freshness_pass_fail_and_empty():
    assert dq.freshness("t", 2.0, 26).status == dq.PASS
    late = dq.freshness("t", 30.5, 26)
    assert late.status == dq.FAIL and "30.5 h" in late.detail
    assert dq.freshness("t", None, 26).status == dq.FAIL


def test_volume_compares_with_median():
    ok = dq.volume("t", 900, [1000] * 7)
    assert ok.status == dq.PASS and ok.measured == 0.9
    low = dq.volume("t", 300, [1000] * 7)
    assert low.status == dq.FAIL and "30%" in low.detail


def test_volume_ignores_one_odd_day():
    # One huge day (e.g. a re-delivery) doesn't move the median.
    assert dq.volume("t", 1000, [1000, 1000, 1000, 900_000, 1000]).status == dq.PASS


def test_volume_detail_names_the_day():
    check = dq.volume("t", 1152, [1152] * 7, day="2026-11-25")
    assert "on 2026-11-25" in check.detail and "median 1152" in check.detail


def test_volume_needs_history_and_nonzero_average():
    assert dq.volume("t", 0, [1000, 1000]).status == dq.WARN
    assert dq.volume("t", 0, [0, 0, 0, 0]).status == dq.WARN


def test_failures_and_rows():
    checks = [dq.freshness("a", 1, 2), dq.freshness("b", 5, 2)]
    assert [c.table_name for c in dq.failures(checks)] == ["b"]
    assert set(checks[0].as_row()) == {
        "check_type",
        "table_name",
        "measured",
        "threshold",
        "status",
        "detail",
    }
