"""Tests for the register change simulator (P3.2)."""

from __future__ import annotations

import os
import random
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ingestion.batch import mastr_simulator as sim
from ingestion.batch.mastr_load import DDL_PATH, connect, sql_statements

DROP_ALL = (
    "DROP TABLE IF EXISTS public.power_units, public.simulator_runs, public.power_units_small_agg"
)


def test_new_unit_id_is_marked_and_unique_per_index():
    run_at = datetime(2026, 11, 23, 6, 17, tzinfo=UTC)
    assert sim.new_unit_id("solar", run_at, 1) == "SIMSEE202611230617001"
    assert sim.new_unit_id("wind", run_at, 12) == "SIMSEW202611230617012"
    assert set(sim.ID_PREFIX) == set(sim.NEW_UNIT_WEIGHTS)


def test_capacity_factor_is_an_upgrade():
    rng = random.Random(1)
    factors = [sim.capacity_factor(rng) for _ in range(200)]
    assert all(Decimal("1.05") <= f <= Decimal("1.50") for f in factors)


def test_main_requires_dsn(monkeypatch):
    monkeypatch.delenv(sim.DSN_ENV, raising=False)
    with pytest.raises(SystemExit):
        sim.main([])


TEST_DSN = os.environ.get("GRIDPULSE_TEST_PG_DSN")


def _seed_register(conn) -> None:
    conn.run(DROP_ALL)
    for statement in sql_statements(DDL_PATH.read_text(encoding="utf-8")):
        conn.run(statement)
    statuses = [sim.IN_OPERATION] * 30 + [sim.PLANNED] * 15 + [sim.TEMP_STOPPED] * 5
    for i, status in enumerate(statuses):
        unit_type = list(sim.NEW_UNIT_WEIGHTS)[i % len(sim.NEW_UNIT_WEIGHTS)]
        conn.run(
            "INSERT INTO public.power_units (unit_id, unit_type, energy_source, federal_state, "
            "postcode, net_capacity_kw, status, updated_at) VALUES "
            "(:id, :t, 'X', 'Hessen', '36037', 1000, :s, '2026-11-22 20:00+00')",
            id=f"U{i:03d}",
            t=unit_type,
            s=status,
        )


@pytest.mark.skipif(not TEST_DSN, reason="set GRIDPULSE_TEST_PG_DSN to run against Postgres")
def test_simulate_changes_rows_and_bumps_watermark():
    conn = connect(TEST_DSN)
    try:
        _seed_register(conn)
        result = sim.simulate(conn, sim.Plan(), seed=7)
        assert result == sim.RunResult(15, 10, 8, 3, 3, 5)

        [[changed]] = conn.run(
            "SELECT count(*) FROM public.power_units WHERE updated_at > '2026-11-22 20:00+00'"
        )
        assert changed == result.total  # every change bumped updated_at, nothing else did
        [[wrong_source]] = conn.run(
            "SELECT count(*) FROM public.power_units "
            "WHERE updated_at > '2026-11-22 20:00+00' AND change_source <> 'simulator'"
        )
        assert wrong_source == 0
        [[new]] = conn.run("SELECT count(*) FROM public.power_units WHERE unit_id LIKE 'SIM%'")
        assert new == 15
        [[closed_without_date]] = conn.run(
            "SELECT count(*) FROM public.power_units "
            "WHERE status = :s AND decommissioning_date IS NULL",
            s=sim.CLOSED,
        )
        assert closed_without_date == 0
        [[upgraded]] = conn.run(
            "SELECT count(*) FROM public.power_units WHERE net_capacity_kw > 1000 "
            "AND unit_id NOT LIKE 'SIM%'"
        )
        assert upgraded == 8
        [[runs]] = conn.run("SELECT count(*) FROM public.simulator_runs")
        assert runs == 1
    finally:
        conn.run(DROP_ALL)
        conn.close()


@pytest.mark.skipif(not TEST_DSN, reason="set GRIDPULSE_TEST_PG_DSN to run against Postgres")
def test_simulate_rolls_back_on_error(monkeypatch):
    conn = connect(TEST_DSN)
    try:
        _seed_register(conn)

        def boom(*args, **kwargs):
            raise RuntimeError("simulated failure")

        monkeypatch.setattr(sim, "_insert_new_units", boom)
        with pytest.raises(RuntimeError):
            sim.simulate(conn, sim.Plan(), seed=1)
        [[changed]] = conn.run(
            "SELECT count(*) FROM public.power_units WHERE updated_at > '2026-11-22 20:00+00'"
        )
        assert changed == 0  # half-done run left no trace
    finally:
        conn.run(DROP_ALL)
        conn.close()
