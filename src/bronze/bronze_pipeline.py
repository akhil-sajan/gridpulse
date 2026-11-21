"""Bronze layer (P1.5): raw landed files -> streaming tables, one row per file.

Auto Loader picks up new files from the landing volume incrementally. The payload is
kept exactly as the source sent it; bronze only adds lineage columns:

- where the row came from (``_source_file``, ``_file_modified_at``, ``_ingest_date``),
- when it was loaded (``_ingested_at``),
- what the file name says about its content (series, week, location, partial or final).

Unexpected fields in a file go to ``_rescued_data`` instead of breaking the load.

Tables (catalog from the bundle target, schema ``bronze``):
    smard_generation, smard_consumption, smard_prices
    openmeteo_archive, openmeteo_forecast
"""

import dlt
from pyspark.sql import functions as F

LANDING_ROOT = spark.conf.get("gridpulse.landing_root")

SMARD_SCHEMA = """
    meta_data STRUCT<version: BIGINT, created: BIGINT>,
    series ARRAY<ARRAY<DOUBLE>>
"""

OPEN_METEO_SCHEMA = """
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    generationtime_ms DOUBLE,
    utc_offset_seconds BIGINT,
    timezone STRING,
    timezone_abbreviation STRING,
    hourly_units STRUCT<
        time: STRING,
        shortwave_radiation: STRING,
        cloud_cover: STRING,
        wind_speed_100m: STRING,
        temperature_2m: STRING>,
    hourly STRUCT<
        time: ARRAY<STRING>,
        shortwave_radiation: ARRAY<DOUBLE>,
        cloud_cover: ARRAY<DOUBLE>,
        wind_speed_100m: ARRAY<DOUBLE>,
        temperature_2m: ARRAY<DOUBLE>>
"""

# File-name patterns written by the ingesters (see ingestion/batch/*.py).
SMARD_NAME = r"^([a-z_]+?)_(\d+)_([A-Za-z-]+)_([a-z]+)_(\d+)(?:_partial_(\d{8}T\d{4}Z))?\.json$"
ARCHIVE_NAME = r"^([a-z_]+?)_(\d{4}-\d{2})(?:_partial_(\d{8}T\d{4}Z))?\.json$"
FORECAST_NAME = r"^([a-z_]+?)_forecast_(\d{8}T\d{4}Z)\.json$"
STAMP_FORMAT = "yyyyMMdd'T'HHmm'Z'"


def read_landing(folder: str, schema: str):
    """Incrementally read every JSON file under one landing folder with Auto Loader."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(f"{LANDING_ROOT}/{folder}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_file_name"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.to_date(
                F.regexp_extract("_metadata.file_path", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
            ).alias("_ingest_date"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


def fetched_at(col):
    return F.when(col != "", F.to_timestamp(col, STAMP_FORMAT))


def smard_table(dataset: str):
    @dlt.table(
        name=f"smard_{dataset}",
        comment=f"Raw SMARD {dataset} weekly files, one row per landed file.",
        table_properties={"quality": "bronze"},
    )
    @dlt.expect("file_name_recognised", "series_name <> ''")
    def _table():
        name = F.col("_file_name")
        partial = F.regexp_extract(name, SMARD_NAME, 6)
        return read_landing(f"smard/{dataset}", SMARD_SCHEMA).select(
            "*",
            F.regexp_extract(name, SMARD_NAME, 1).alias("series_name"),
            F.regexp_extract(name, SMARD_NAME, 2).cast("int").alias("filter_id"),
            F.regexp_extract(name, SMARD_NAME, 3).alias("region"),
            F.regexp_extract(name, SMARD_NAME, 4).alias("resolution"),
            F.timestamp_millis(F.regexp_extract(name, SMARD_NAME, 5).cast("bigint")).alias(
                "week_start_utc"
            ),
            (partial != "").alias("is_partial"),
            fetched_at(partial).alias("partial_fetched_at"),
        )


for _dataset in ("generation", "consumption", "prices"):
    smard_table(_dataset)


@dlt.table(
    name="openmeteo_archive",
    comment="Raw Open-Meteo hourly history, one row per location and month file.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("file_name_recognised", "location_code <> ''")
def openmeteo_archive():
    name = F.col("_file_name")
    partial = F.regexp_extract(name, ARCHIVE_NAME, 3)
    return read_landing("open_meteo/archive", OPEN_METEO_SCHEMA).select(
        "*",
        F.regexp_extract(name, ARCHIVE_NAME, 1).alias("location_code"),
        F.to_date(F.concat(F.regexp_extract(name, ARCHIVE_NAME, 2), F.lit("-01"))).alias(
            "month_start"
        ),
        (partial != "").alias("is_partial"),
        fetched_at(partial).alias("partial_fetched_at"),
    )


@dlt.table(
    name="openmeteo_forecast",
    comment="Raw Open-Meteo 48-hour forecasts, one row per location and fetch.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("file_name_recognised", "location_code <> ''")
def openmeteo_forecast():
    name = F.col("_file_name")
    return read_landing("open_meteo/forecast", OPEN_METEO_SCHEMA).select(
        "*",
        F.regexp_extract(name, FORECAST_NAME, 1).alias("location_code"),
        fetched_at(F.regexp_extract(name, FORECAST_NAME, 2)).alias("fetched_at"),
    )
