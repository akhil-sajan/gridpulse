"""Silver layer (P2.1): unpacked, typed, deduplicated, quality-checked.

Bronze holds one row per landed file with the values packed in arrays. Silver:

1. unpacks every array into one row per interval (15 min for SMARD, 1 h for weather),
2. keys every row by a UTC instant and adds German local time and date as attributes
   (local days have 92, 96 or 100 quarter-hours because of daylight saving time),
3. keeps only the latest version of each value with AUTO CDC (SCD type 1): open weeks
   and months are landed several times as partial files, then once as final; the file
   landed last wins,
4. drops rows that break a quality rule and keeps them in a quarantine table instead.

Values that the source hasn't published yet (nulls in open weeks/months) are not rows.

Tables (catalog from the bundle target, schema ``silver``):
    smard_generation, smard_consumption, smard_prices, smard_quarantine
    openmeteo_weather, openmeteo_forecast, openmeteo_quarantine

Bronze tables are read by their names in this pipeline (default schema ``bronze``).
The pipeline sets the Spark session time zone to UTC, so timestamps are interpreted
and stored as UTC.
"""

from functools import reduce

import dlt
from pyspark.sql import Column
from pyspark.sql import functions as F

CATALOG = spark.conf.get("gridpulse.catalog")
LOCAL_TZ = "Europe/Berlin"


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


def guarded(rules: dict[str, str]) -> dict[str, str]:
    """Treat a rule that evaluates to NULL as failed."""
    return {name: f"coalesce({expr}, false)" for name, expr in rules.items()}


def failed_rules(rules: dict[str, str]) -> Column:
    """Array with the names of the rules a row breaks."""
    checks = [F.when(~F.expr(expr), F.lit(name)) for name, expr in guarded(rules).items()]
    return F.array_compact(F.array(*checks))


def all_pass(rules: dict[str, str]) -> Column:
    return reduce(lambda a, b: a & b, [F.expr(e) for e in guarded(rules).values()])


def with_local_time(df):
    local = F.from_utc_timestamp("ts_utc", LOCAL_TZ)
    return df.withColumn("ts_local", local).withColumn("local_date", F.to_date(local))


# --------------------------------------------------------------------------- SMARD

SMARD_COMMON_RULES = {
    "ts_present": "ts_utc IS NOT NULL",
    "aligned_to_resolution": (
        "second(ts_utc) = 0 AND ("
        "(resolution = 'quarterhour' AND minute(ts_utc) % 15 = 0) OR "
        "(resolution = 'hour' AND minute(ts_utc) = 0))"
    ),
}
SMARD_RULES = {
    "generation": {**SMARD_COMMON_RULES, "non_negative": "value >= 0"},
    "consumption": {
        **SMARD_COMMON_RULES,
        # Residual load (load minus wind and solar) can be negative; grid load cannot.
        "grid_load_non_negative": "series_name <> 'grid_load' OR value >= 0",
    },
    # EU day-ahead price limits: -500 to 4,000 EUR/MWh.
    "prices": {**SMARD_COMMON_RULES, "price_in_market_limits": "value BETWEEN -500 AND 4000"},
}
SMARD_UNIT = {"generation": "MWh", "consumption": "MWh", "prices": "EUR/MWh"}


def smard_points(dataset: str):
    """One row per published value of a SMARD bronze table."""
    return (
        dlt.read_stream(f"smard_{dataset}")
        .select(
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
            F.lit(SMARD_UNIT[dataset]).alias("unit"),
            "_source_file",
            "_file_modified_at",
        )
        .where(F.col("value").isNotNull())
    )


def define_smard(dataset: str) -> None:
    rules = SMARD_RULES[dataset]

    @dlt.view(name=f"smard_{dataset}_points")
    def _points():
        return smard_points(dataset)

    @dlt.view(name=f"smard_{dataset}_valid")
    @dlt.expect_all_or_drop(guarded(rules))
    def _valid():
        return with_local_time(dlt.read_stream(f"smard_{dataset}_points"))

    dlt.create_streaming_table(
        name=silver(f"smard_{dataset}"),
        comment=(
            f"SMARD {dataset}: one row per series, resolution and UTC interval start; "
            "latest landed version wins."
        ),
        table_properties={"quality": "silver"},
    )
    dlt.apply_changes(
        target=silver(f"smard_{dataset}"),
        source=f"smard_{dataset}_valid",
        keys=["series_name", "resolution", "ts_utc"],
        sequence_by=F.col("_file_modified_at"),
        stored_as_scd_type=1,
    )


for _dataset in ("generation", "consumption", "prices"):
    define_smard(_dataset)


@dlt.table(
    name=silver("smard_quarantine"),
    comment="SMARD rows that broke a silver quality rule, with the rules they broke.",
    table_properties={"quality": "quarantine"},
)
def smard_quarantine():
    parts = [
        dlt.read_stream(f"smard_{ds}_points")
        .withColumn("failed_rules", failed_rules(rules))
        .where(~all_pass(rules))
        for ds, rules in SMARD_RULES.items()
    ]
    return reduce(lambda a, b: a.unionByName(b), parts).withColumn(
        "_quarantined_at", F.current_timestamp()
    )


# --------------------------------------------------------------------------- Open-Meteo

WEATHER_VARS = ("shortwave_radiation", "cloud_cover", "wind_speed_100m", "temperature_2m")
WEATHER_RULES = {
    "ts_present": "ts_utc IS NOT NULL",
    "radiation_non_negative": "shortwave_radiation IS NULL OR shortwave_radiation >= 0",
    "cloud_cover_is_percent": "cloud_cover IS NULL OR cloud_cover BETWEEN 0 AND 100",
    "wind_non_negative": "wind_speed_100m IS NULL OR wind_speed_100m >= 0",
    "temperature_plausible": "temperature_2m IS NULL OR temperature_2m BETWEEN -50 AND 50",
}


def weather_points(table: str, extra_cols: tuple[str, ...]):
    """One row per location and hour of an Open-Meteo bronze table (UTC times)."""
    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{v}").alias(v) for v in WEATHER_VARS],
    )
    return (
        dlt.read_stream(table)
        .select(
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
        # Hours with no values yet (data not published) are not rows.
        .where(reduce(lambda a, b: a | b, [F.col(v).isNotNull() for v in WEATHER_VARS]))
    )


@dlt.view(name="openmeteo_archive_points")
def openmeteo_archive_points():
    return weather_points("openmeteo_archive", ())


@dlt.view(name="openmeteo_archive_valid")
@dlt.expect_all_or_drop(guarded(WEATHER_RULES))
def openmeteo_archive_valid():
    return with_local_time(dlt.read_stream("openmeteo_archive_points"))


dlt.create_streaming_table(
    name=silver("openmeteo_weather"),
    comment="Observed hourly weather per location (UTC hour start); latest landed version wins.",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target=silver("openmeteo_weather"),
    source="openmeteo_archive_valid",
    keys=["location_code", "ts_utc"],
    sequence_by=F.col("_file_modified_at"),
    stored_as_scd_type=1,
)


@dlt.view(name="openmeteo_forecast_points")
def openmeteo_forecast_points():
    return weather_points("openmeteo_forecast", ("fetched_at",))


@dlt.table(
    name=silver("openmeteo_forecast"),
    comment=(
        "Hourly weather forecasts per location, one row per fetch and forecast hour; "
        "every fetch is kept so forecast accuracy can be measured later."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(guarded(WEATHER_RULES))
def openmeteo_forecast():
    return with_local_time(dlt.read_stream("openmeteo_forecast_points")).withColumn(
        "lead_hours",
        ((F.unix_timestamp("ts_utc") - F.unix_timestamp("fetched_at")) / 3600).cast("int"),
    )


@dlt.table(
    name=silver("openmeteo_quarantine"),
    comment="Weather rows that broke a silver quality rule, with the rules they broke.",
    table_properties={"quality": "quarantine"},
)
def openmeteo_quarantine():
    archive = dlt.read_stream("openmeteo_archive_points").withColumn(
        "source_table", F.lit("archive")
    )
    forecast = (
        dlt.read_stream("openmeteo_forecast_points")
        .drop("fetched_at")
        .withColumn("source_table", F.lit("forecast"))
    )
    return (
        archive.unionByName(forecast)
        .withColumn("failed_rules", failed_rules(WEATHER_RULES))
        .where(~all_pass(WEATHER_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
