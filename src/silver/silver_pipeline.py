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

import sys
from functools import reduce

import dlt
from pyspark.sql import functions as F

# The bundle root (set in bronze.pipeline.yml) makes the shared code in src/utils importable.
_root = spark.conf.get("gridpulse.repo_root", "")
if _root and _root not in sys.path:
    sys.path.insert(0, _root)

from src.utils.transforms import (  # noqa: E402
    all_pass,
    failed_rules,
    guarded,
    smard_points,
    weather_points,
    with_local_time,
)

CATALOG = spark.conf.get("gridpulse.catalog")


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


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


def define_smard(dataset: str) -> None:
    rules = SMARD_RULES[dataset]

    @dlt.view(name=f"smard_{dataset}_points")
    def _points():
        return smard_points(dlt.read_stream(f"smard_{dataset}"), dataset, SMARD_UNIT[dataset])

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

WEATHER_RULES = {
    "ts_present": "ts_utc IS NOT NULL",
    "radiation_non_negative": "shortwave_radiation IS NULL OR shortwave_radiation >= 0",
    "cloud_cover_is_percent": "cloud_cover IS NULL OR cloud_cover BETWEEN 0 AND 100",
    "wind_non_negative": "wind_speed_100m IS NULL OR wind_speed_100m >= 0",
    "temperature_plausible": "temperature_2m IS NULL OR temperature_2m BETWEEN -50 AND 50",
}


@dlt.view(name="openmeteo_archive_points")
def openmeteo_archive_points():
    return weather_points(dlt.read_stream("openmeteo_archive"))


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
    return weather_points(dlt.read_stream("openmeteo_forecast"), ("fetched_at",))


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


# --------------------------------------------------------------------- reference data (P2.2)

# One row per SMARD generation series. Categories follow how SMARD groups sources:
# renewable, fossil (incl. "other conventional": oil, mixed fuels, non-renewable waste),
# nuclear, and storage (pumped storage moves energy in time rather than producing it).
PRODUCTION_TYPES = [
    # series_name, filter, English name, German name, category, renewable, weather-dependent
    ("solar", 4068, "Solar", "Photovoltaik", "renewable", True, True),
    ("wind_onshore", 4067, "Wind onshore", "Wind Onshore", "renewable", True, True),
    ("wind_offshore", 1225, "Wind offshore", "Wind Offshore", "renewable", True, True),
    ("biomass", 4066, "Biomass", "Biomasse", "renewable", True, False),
    ("hydro", 1226, "Hydropower", "Wasserkraft", "renewable", True, False),
    ("other_renewable", 1228, "Other renewable", "Sonstige Erneuerbare", "renewable", True, False),
    ("lignite", 1223, "Lignite", "Braunkohle", "fossil", False, False),
    ("hard_coal", 4069, "Hard coal", "Steinkohle", "fossil", False, False),
    ("natural_gas", 4071, "Natural gas", "Erdgas", "fossil", False, False),
    (
        "other_conventional",
        1227,
        "Other conventional",
        "Sonstige Konventionelle",
        "fossil",
        False,
        False,
    ),
    ("nuclear", 1224, "Nuclear", "Kernenergie", "nuclear", False, False),
    ("pumped_storage", 4070, "Pumped storage", "Pumpspeicher", "storage", False, False),
]


@dlt.table(
    name=silver("ref_production_type"),
    comment=(
        "Reference: SMARD generation series -> category (renewable, fossil, nuclear, storage), "
        "renewable flag and weather dependence."
    ),
    table_properties={"quality": "silver"},
)
def ref_production_type():
    return spark.createDataFrame(
        PRODUCTION_TYPES,
        "series_name STRING, smard_filter_id INT, name_en STRING, name_de STRING, "
        "category STRING, is_renewable BOOLEAN, is_weather_dependent BOOLEAN",
    )


# ------------------------------------------------------------------- national weather (P2.3)


@dlt.table(
    name=silver("weather_national_hourly"),
    comment=(
        "Germany-wide hourly weather: plain average over the 16 state locations "
        "(capacity weighting follows in Phase 3). Hourly, UTC; energy data is 15-minute."
    ),
    table_properties={"quality": "silver"},
)
def weather_national_hourly():
    return (
        spark.read.table(silver("openmeteo_weather"))
        .groupBy("ts_utc")
        .agg(
            F.avg("shortwave_radiation").alias("shortwave_radiation"),
            F.avg("cloud_cover").alias("cloud_cover"),
            F.avg("wind_speed_100m").alias("wind_speed_100m"),
            F.avg("temperature_2m").alias("temperature_2m"),
            F.count("*").alias("n_locations"),
        )
        .transform(with_local_time)
    )
