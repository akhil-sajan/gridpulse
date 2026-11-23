"""Tests for the pure parts of the incremental MaStR extract (P3.3)."""

from __future__ import annotations

from datetime import datetime

import pytest

from ingestion.batch.mastr_load import COLUMNS as LOADER_COLUMNS
from src.cdc import mastr_extract as x


def test_first_run_reads_everything_up_to_window_end():
    q = x.build_query(None, datetime(2026, 11, 24, 4, 29, 0, 123456))
    assert q.endswith("WHERE updated_at <= TIMESTAMPTZ '2026-11-24 04:29:00.123456+00'")
    assert ">" not in q


def test_later_runs_read_half_open_window():
    q = x.build_query(datetime(2026, 11, 23, 4, 29), datetime(2026, 11, 24, 4, 29))
    assert (
        "WHERE updated_at > TIMESTAMPTZ '2026-11-23 04:29:00.000000+00' "
        "AND updated_at <= TIMESTAMPTZ '2026-11-24 04:29:00.000000+00'"
    ) in q


def test_extract_reads_every_register_column():
    assert set(LOADER_COLUMNS) | {"change_source", "updated_at"} == set(x.COLUMNS)
    for column in x.COLUMNS:
        assert column in x.BRONZE_DDL


def test_jdbc_options_from_dsn():
    opts = x.jdbc_options(
        "postgresql://neondb_owner:p%40ss@ep-abc-123.eu-central-1.aws.neon.tech/neondb"
        "?sslmode=require&channel_binding=require"
    )
    assert opts["url"] == (
        "jdbc:postgresql://ep-abc-123.eu-central-1.aws.neon.tech:5432/neondb?sslmode=require"
    )
    assert opts["user"] == "neondb_owner"
    assert opts["password"] == "p@ss"


def test_jdbc_options_rejects_other_urls():
    with pytest.raises(ValueError):
        x.jdbc_options("mysql://u:p@h/db")
