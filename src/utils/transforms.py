"""Spark transformations shared by the Lakeflow pipeline files (P5.3).

Each function takes a DataFrame and returns a DataFrame. Nothing here imports ``dlt``
or uses the notebook ``spark`` global, so the same code runs in the pipeline and in
``tests/test_transforms.py`` on a local SparkSession.

The pipeline files only add the Lakeflow parts: where the data is read from
(``dlt.read_stream``), expectations, and the target tables.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LOCAL_TZ = "Europe/Berlin"

# --------------------------------------------------------------------------- rules


def guarded(rules: dict[str, str]) -> dict[str, str]:
    """A rule that evaluates to NULL counts as broken (e.g. a field that didn't parse)."""
    return {name: f"coalesce({expr}, false)" for name, expr in rules.items()}


def failed_rules(rules: dict[str, str]) -> Column:
    """Array with the names of the rules a row breaks (empty if it passes all)."""
    checks = [F.when(~F.expr(expr), F.lit(name)) for name, expr in guarded(rules).items()]
    return F.array_compact(F.array(*checks))


def all_pass(rules: dict[str, str]) -> Column:
    """True only if the row passes every rule."""
    return reduce(lambda a, b: a & b, [F.expr(e) for e in guarded(rules).values()])


# --------------------------------------------------------------------------- time


def with_local_time(df: DataFrame, tz: str = LOCAL_TZ) -> DataFrame:
    """Add German local time and local date as attributes of the UTC key ``ts_utc``.

    Requires the session time zone to be UTC (the pipeline sets it)."""
    local = F.from_utc_timestamp("ts_utc", tz)
    return df.withColumn("ts_local", local).withColumn("local_date", F.to_date(local))


# --------------------------------------------------------------------------- unpivot


def smard_points(bronze: DataFrame, dataset: str, unit: str) -> DataFrame:
    """SMARD bronze (one row per file, ``series`` = array of [epoch_ms, value]) ->
    one row per published value. Values not published yet (NULL) are not rows."""
    return (
        bronze.select(
            "series_name",
            "filter_id",
            "region",
            "resolution",
            "_source_file",
            "_file_modified_at",
            F.explode("series").alias("point"),
        )
        .select(
            F.lit(dataset).alias("dataset"),
            "series_name",
            "filter_id",
            "region",
            "resolution",
            F.timestamp_millis(F.col("point")[0].cast("bigint")).alias("ts_utc"),
            F.col("point")[1].alias("value"),
            F.lit(unit).alias("unit"),
            "_source_file",
            "_file_modified_at",
        )
        .where(F.col("value").isNotNull())
    )


WEATHER_VARS = ("shortwave_radiation", "cloud_cover", "wind_speed_100m", "temperature_2m")


def weather_points(bronze: DataFrame, extra_cols: tuple[str, ...] = ()) -> DataFrame:
    """Open-Meteo bronze (parallel arrays under ``hourly``) -> one row per location and
    UTC hour. Hours where every variable is still NULL are not rows."""
    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{v}").alias(v) for v in WEATHER_VARS],
    )
    return (
        bronze.select(
            "location_code",
            *extra_cols,
            "latitude",
            "longitude",
            "_source_file",
            "_file_modified_at",
            F.explode(zipped).alias("p"),
        )
        .select(
            "location_code",
            *extra_cols,
            "latitude",
            "longitude",
            F.to_timestamp(
                F.concat(F.col("p.time"), F.lit(":00+00:00")), "yyyy-MM-dd'T'HH:mm:ssXXX"
            ).alias("ts_utc"),
            *[F.col(f"p.{v}").alias(v) for v in WEATHER_VARS],
            "_source_file",
            "_file_modified_at",
        )
        .where(reduce(lambda a, b: a | b, [F.col(v).isNotNull() for v in WEATHER_VARS]))
    )


# --------------------------------------------------------------------------- stream

ENVELOPE = """
    event_id STRING,
    event_ts_utc STRING,
    produced_at STRING,
    schema_version STRING,
    source STRING,
    payload STRING
"""


def parse_events(raw: DataFrame, payload_schema: str, schema_version: str) -> DataFrame:
    """Raw stream rows (``value`` = JSON text) -> envelope + payload columns.

    Keeps the raw text and the log position. A message that doesn't parse keeps NULLs,
    so it fails the rules and ends up in quarantine. ``schema_ok`` is False for any
    other schema version."""
    env = F.from_json("value", ENVELOPE)
    return (
        raw.withColumn("e", env)
        .withColumn("p", F.from_json(F.col("e.payload"), payload_schema))
        .select(
            F.col("e.event_id").alias("event_id"),
            F.col("e.event_ts_utc").cast("timestamp").alias("event_ts_utc"),
            F.col("e.produced_at").cast("timestamp").alias("produced_at"),
            F.col("p.*"),
            F.col("e.schema_version").alias("schema_version"),
            F.col("value").alias("raw_value"),
            "kafka_timestamp",
            "_ingested_at",
            "topic",
            "partition",
            "offset",
        )
        .withColumn(
            "schema_ok", F.coalesce(F.col("schema_version") == schema_version, F.lit(False))
        )
    )


HELPER_COLUMNS = ("raw_value", "schema_version", "schema_ok")


def dedup_events(valid: DataFrame, time_col: str, delay: str) -> DataFrame:
    """Drop the helper columns and exact re-sends (same ``event_id``) within the
    watermark ``delay`` on ``time_col``. A replay therefore adds nothing to silver."""
    return (
        valid.drop(*HELPER_COLUMNS)
        .withWatermark(time_col, delay)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def meter_consumption_30min(readings: DataFrame) -> DataFrame:
    """Silver meter readings -> total kWh, meters and readings per UTC half-hour."""
    return (
        readings.groupBy(F.window("event_ts_utc", "30 minutes").alias("w"))
        .agg(
            F.round(F.sum("energy_kwh"), 3).alias("total_kwh"),
            F.countDistinct("meter_id").alias("meters"),
            F.count("*").alias("readings"),
            F.max("_ingested_at").alias("_last_ingested_at"),
        )
        .select(
            F.col("w.start").alias("window_start_utc"),
            F.col("w.end").alias("window_end_utc"),
            "total_kwh",
            "meters",
            "readings",
            "_last_ingested_at",
        )
    )
