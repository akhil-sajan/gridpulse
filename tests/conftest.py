"""Shared fixtures. ``spark``: one local SparkSession for the whole test run (P5.3).

Spark tests are skipped when PySpark (or Java) isn't installed, so the rest of the
suite still runs on a laptop without them. CI installs both.
"""

from __future__ import annotations

import os
import time

import pytest

# PySpark turns collected timestamps into naive datetimes in the process time zone;
# pin it to UTC so the expected values are the same on every machine.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("gridpulse-tests")
        # Same as the pipeline: timestamps are UTC.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
