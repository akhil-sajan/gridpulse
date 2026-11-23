"""Incremental extract of the power-plant register from Postgres (P3.3).

Watermark-based change data capture (CDC): each run reads only the rows of
``public.power_units`` whose ``updated_at`` lies in a new time window, with Spark JDBC,
and appends them unchanged to ``<catalog>.bronze.mastr_power_units_changes``.

The window is ``(last watermark, now - safety lag]``:

* **lower bound** = the upper bound of the last successful run, from
  ``<catalog>.ops.ingest_watermarks`` (none on the first run: full extract).
* **upper bound** = the database's own clock minus one minute. Postgres sets
  ``updated_at = now()`` to the *start* of the writing transaction, so a transaction that
  started just before the extract but commits just after it would otherwise be skipped
  forever. Reading only up to a minute ago leaves time for such commits to land.

The watermark row is written *after* the data, so a crash in between re-reads the same
window next time. Bronze may then hold a row twice; that is harmless, because the SCD2
step (P3.4) keys on ``unit_id`` and sequences by ``updated_at`` (at-least-once delivery,
idempotent downstream).

Limits of watermark CDC: hard deletes are invisible (the register uses status changes
instead), and several changes to one row between two runs arrive as one final state.

Run as a Databricks job task (spark_python_task)::

    mastr_extract.py <catalog> [--secret-scope gridpulse] [--secret-key pg_dsn]
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime
from urllib.parse import unquote, urlparse

SOURCE = "mastr_power_units"
SAFETY_LAG = "1 minute"
COLUMNS = (
    "unit_id",
    "unit_type",
    "energy_source",
    "federal_state",
    "postcode",
    "net_capacity_kw",
    "commissioning_date",
    "decommissioning_date",
    "status",
    "mastr_updated_at",
    "change_source",
    "updated_at",
)

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    unit_id STRING NOT NULL,
    unit_type STRING,
    energy_source STRING,
    federal_state STRING,
    postcode STRING,
    net_capacity_kw DECIMAL(12, 3),
    commissioning_date DATE,
    decommissioning_date DATE,
    status STRING,
    mastr_updated_at TIMESTAMP,
    change_source STRING,
    updated_at TIMESTAMP NOT NULL,
    _window_from TIMESTAMP COMMENT 'Exclusive lower bound of the extract window (NULL = full)',
    _window_to TIMESTAMP NOT NULL COMMENT 'Inclusive upper bound of the extract window',
    _extract_id STRING NOT NULL COMMENT 'One ID per extract run',
    _extracted_at TIMESTAMP NOT NULL
)
COMMENT 'Changed rows of public.power_units (Neon Postgres), appended per watermark window'
CLUSTER BY (unit_id)
"""

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    source STRING NOT NULL,
    window_from TIMESTAMP,
    window_to TIMESTAMP NOT NULL,
    rows_extracted BIGINT NOT NULL,
    run_at TIMESTAMP NOT NULL
)
COMMENT 'One row per incremental extract run; the latest window_to is the current watermark'
"""


def pg_timestamp(ts: datetime) -> str:
    """Format a UTC timestamp (naive, as Spark returns it) as a Postgres literal."""
    return f"{ts:%Y-%m-%d %H:%M:%S.%f}+00"


def build_query(window_from: datetime | None, window_to: datetime) -> str:
    """SQL that Postgres runs: only rows changed inside the window."""
    cond = f"updated_at <= TIMESTAMPTZ '{pg_timestamp(window_to)}'"
    if window_from is not None:
        cond = f"updated_at > TIMESTAMPTZ '{pg_timestamp(window_from)}' AND {cond}"
    return f"SELECT {', '.join(COLUMNS)} FROM public.power_units WHERE {cond}"


def jdbc_options(dsn: str) -> dict[str, str]:
    """Spark JDBC options from a postgresql:// connection string (TLS required)."""
    url = urlparse(dsn)
    if url.scheme not in ("postgres", "postgresql") or not url.hostname:
        raise ValueError("expected a postgresql:// connection string")
    return {
        "url": f"jdbc:postgresql://{url.hostname}:{url.port or 5432}{url.path}?sslmode=require",
        "user": unquote(url.username or ""),
        "password": unquote(url.password or ""),
        "fetchsize": "10000",
    }


def run(catalog: str, scope: str, key: str) -> int:
    from databricks.sdk.runtime import dbutils
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    options = jdbc_options(dbutils.secrets.get(scope, key))
    bronze = f"{catalog}.bronze.mastr_power_units_changes"
    watermarks = f"{catalog}.ops.ingest_watermarks"
    spark.sql(BRONZE_DDL.format(table=bronze))
    spark.sql(WATERMARK_DDL.format(table=watermarks))

    [[window_from]] = spark.sql(
        f"SELECT max(window_to) FROM {watermarks} WHERE source = '{SOURCE}'"
    ).collect()
    window_to = (
        spark.read.format("jdbc")
        .options(**options)
        .option("query", f"SELECT (now() - interval '{SAFETY_LAG}') AT TIME ZONE 'UTC' AS t")
        .load()
        .first()["t"]
    )
    if window_from is not None and window_to <= window_from:
        print(f"Nothing to do: watermark {window_from} is newer than {window_to}")
        return 0

    extract_id = str(uuid.uuid4())
    schema = spark.table(bronze).schema
    changes = (
        spark.read.format("jdbc")
        .options(**options)
        .option("query", build_query(window_from, window_to))
        .load()
        .withColumn("_window_from", F.lit(window_from).cast("timestamp"))
        .withColumn("_window_to", F.lit(window_to).cast("timestamp"))
        .withColumn("_extract_id", F.lit(extract_id))
        .withColumn("_extracted_at", F.current_timestamp())
    )
    # Read Postgres once: write straight to bronze, then count what this run wrote.
    changes.select(*[F.col(f.name).cast(f.dataType) for f in schema]).write.mode(
        "append"
    ).saveAsTable(bronze)
    written = spark.table(bronze).where(F.col("_extract_id") == extract_id)
    rows = written.count()

    # Watermark last: if anything above failed, the same window is read again next time.
    spark.createDataFrame(
        [(SOURCE, window_from, window_to, rows)],
        "source STRING, window_from TIMESTAMP, window_to TIMESTAMP, rows_extracted BIGINT",
    ).withColumn("run_at", F.current_timestamp()).write.mode("append").saveAsTable(watermarks)

    print(f"Extracted {rows:,} changed rows for window ({window_from}, {window_to}] into {bronze}")
    if rows:
        written.groupBy("change_source").count().orderBy("change_source").show()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Incremental extract of public.power_units.")
    parser.add_argument("catalog")
    parser.add_argument("--secret-scope", default="gridpulse")
    parser.add_argument("--secret-key", default="pg_dsn")
    args = parser.parse_args(argv)
    return run(args.catalog, args.secret_scope, args.secret_key)


if __name__ == "__main__":
    # Only exit on errors: serverless job tasks treat any SystemExit, even 0, as a failure.
    code = main()
    if code:
        sys.exit(code)
