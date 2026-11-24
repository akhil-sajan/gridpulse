"""Silver and gold for the Kafka streams (P4.5).

Bronze keeps every Kafka message as raw text. Here the messages are parsed with an
explicit schema (the same contract the producers validate against), checked, and
deduplicated, and the meter readings are summed per half-hour.

**Meter readings** (``silver.stream_meter_readings``): classic streaming deduplication.
``withWatermark(event_ts_utc, 30 minutes)`` + ``dropDuplicatesWithinWatermark(event_id)``
drops a reading whose ``event_id`` was already seen. Replaying a day that was already
replayed produces the same IDs (deterministic producer IDs), so it adds nothing. The
watermark lets Spark forget old IDs, which keeps the state small. Trade-off: once the
stream has moved past a time, readings for clearly earlier times are treated as late and
dropped; for a replayed historical dataset that is acceptable.

**Generation** (``silver.stream_energy_generation``): latest value wins. energy-charts
sometimes revises a quarter-hour after first publishing it; a revision has a new
``event_id``, so removing duplicates alone would keep both values. AUTO CDC (SCD type 1)
keyed on production type and quarter-hour, sequenced by ``produced_at``, keeps exactly one
row per key: the newest one. Exact re-sends (same ID) are dropped first.

**Gold** (``gold.fact_meter_consumption_30min``): materialized view, total kWh and number
of meters per half-hour, computed from the deduplicated readings.

Latency columns (``produced_at``, ``kafka_timestamp``, ``_ingested_at``) are carried
through for the measurements in P4.6.
"""

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("gridpulse.catalog")


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


def gold(name: str) -> str:
    return f"{CATALOG}.gold.{name}"


ENVELOPE = """
    event_id STRING,
    event_ts_utc STRING,
    produced_at STRING,
    schema_version STRING,
    source STRING,
    payload STRING
"""
GENERATION_PAYLOAD = """
    country STRING,
    production_type STRING,
    production_type_slug STRING,
    power_mw DOUBLE
"""
METER_PAYLOAD = """
    meter_id STRING,
    energy_kwh DOUBLE,
    interval_minutes INT,
    replay_id STRING
"""

COMMON_RULES = {
    "event_id_present": "event_id IS NOT NULL",
    "event_time_parsed": "event_ts_utc IS NOT NULL",
    "produced_time_parsed": "produced_at IS NOT NULL",
}


def parsed(bronze_table: str, payload_schema: str, schema_version: str):
    """Raw Kafka rows -> typed columns (envelope + payload), keeping log position."""
    env = F.from_json("value", ENVELOPE)
    return (
        dlt.read_stream(bronze_table)
        .withColumn("e", env)
        .withColumn("p", F.from_json(F.col("e.payload"), payload_schema))
        .where(F.col("e.schema_version") == schema_version)
        .select(
            F.col("e.event_id").alias("event_id"),
            F.col("e.event_ts_utc").cast("timestamp").alias("event_ts_utc"),
            F.col("e.produced_at").cast("timestamp").alias("produced_at"),
            F.col("p.*"),
            "kafka_timestamp",
            "_ingested_at",
            "topic",
            "partition",
            "offset",
        )
    )


# --------------------------------------------------------------------------- meters


@dlt.table(
    name=silver("stream_meter_readings"),
    comment=(
        "London smart-meter readings from Kafka, parsed and deduplicated by event_id "
        "(watermark 30 minutes on event time). One row per meter and half-hour."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["event_ts_utc"],
)
@dlt.expect_all_or_drop(
    COMMON_RULES
    | {
        "meter_id_present": "meter_id IS NOT NULL",
        "energy_not_negative": "energy_kwh >= 0",
    }
)
def stream_meter_readings():
    return (
        parsed("kafka_meter_readings", METER_PAYLOAD, "meters.readings.v1")
        .withWatermark("event_ts_utc", "30 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


@dlt.table(
    name=gold("fact_meter_consumption_30min"),
    comment=(
        "Total household consumption per half-hour across the replayed London meters "
        "(kWh), from the deduplicated stream. Replaying the same data again does not "
        "change these totals."
    ),
    table_properties={"quality": "gold"},
)
def fact_meter_consumption_30min():
    return (
        spark.read.table(silver("stream_meter_readings"))
        .groupBy(F.window("event_ts_utc", "30 minutes").alias("w"))
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


# --------------------------------------------------------------------------- generation


@dlt.view(name="stream_energy_generation_events")
@dlt.expect_all_or_drop(
    COMMON_RULES
    | {
        "production_type_present": "production_type_slug IS NOT NULL",
        "power_present": "power_mw IS NOT NULL",
    }
)
def stream_energy_generation_events():
    return (
        parsed("kafka_energy_generation", GENERATION_PAYLOAD, "energy.generation.v1")
        # Exact re-sends (same event_id) carry nothing new.
        .withWatermark("produced_at", "1 hour")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


dlt.create_streaming_table(
    name=silver("stream_energy_generation"),
    comment=(
        "Live German public net generation per production type and quarter-hour "
        "(energy-charts via Kafka). Latest published value wins (AUTO CDC, SCD type 1)."
    ),
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target=silver("stream_energy_generation"),
    source="stream_energy_generation_events",
    keys=["production_type_slug", "event_ts_utc"],
    sequence_by=F.col("produced_at"),
    stored_as_scd_type=1,
)
