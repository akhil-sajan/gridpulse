"""Streaming latency and throughput (P4.6): ``ops.stream_metrics``.

One row per topic and pipeline update (bronze rows that were stored in the same minute).
For every message, the timestamps along its way give four durations:

* ``source_delay``  = produced_at - event_ts_utc     (how late the source publishes;
  energy-charts about 2.5 h; for the 2012-2014 replay it is meaningless and left out)
* ``broker``        = kafka_timestamp - produced_at   (producer -> Redpanda)
* ``pipeline``      = _ingested_at - kafka_timestamp  (Redpanda -> bronze table)
* ``end_to_end``    = _ingested_at - produced_at      (our whole path)

For each, p50 and p95 in seconds. Throughput is how fast the producer sent:
messages / (last - first kafka_timestamp) in that update.

The pipeline is triggered, so ``pipeline`` mostly measures how long messages waited for
the next update: minutes when the pipeline is started right after sending, up to a day
on the daily schedule. The broker part shows what the transport itself costs.

Built from bronze (every message that arrived, duplicates included), because latency is
about transport, not about unique values.
"""

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("gridpulse.catalog")
SOURCES = {
    "kafka_energy_generation": True,  # has a meaningful event time (live data)
    "kafka_meter_readings": False,  # replayed 2012-2014 data: no source delay
}


def seconds(later, earlier):
    return F.unix_millis(later).cast("double") / 1000 - F.unix_millis(earlier).cast("double") / 1000


def timed(table: str, live: bool):
    produced = F.get_json_object("value", "$.produced_at").cast("timestamp")
    event = F.get_json_object("value", "$.event_ts_utc").cast("timestamp")
    df = spark.read.table(f"{CATALOG}.bronze.{table}").select(
        "topic",
        "kafka_timestamp",
        "_ingested_at",
        produced.alias("produced_at"),
        event.alias("event_ts_utc"),
    )
    return df.select(
        "topic",
        F.date_trunc("minute", "_ingested_at").alias("ingested_minute"),
        "kafka_timestamp",
        seconds(F.col("kafka_timestamp"), F.col("produced_at")).alias("broker_s"),
        seconds(F.col("_ingested_at"), F.col("kafka_timestamp")).alias("pipeline_s"),
        seconds(F.col("_ingested_at"), F.col("produced_at")).alias("end_to_end_s"),
        (
            seconds(F.col("produced_at"), F.col("event_ts_utc"))
            if live
            else F.lit(None).cast("double")
        ).alias("source_delay_s"),
    )


def pct(col: str, p: float):
    return F.round(F.percentile_approx(col, p), 3)


@dlt.table(
    name=f"{CATALOG}.ops.stream_metrics",
    comment=(
        "Kafka stream latency (p50/p95 seconds: source delay, broker, pipeline, end to end) "
        "and producer throughput per topic and pipeline update."
    ),
    table_properties={"quality": "ops"},
)
def stream_metrics():
    frames = [timed(table, live) for table, live in SOURCES.items()]
    union = frames[0]
    for other in frames[1:]:
        union = union.unionByName(other)
    return (
        union.groupBy("topic", "ingested_minute")
        .agg(
            F.count("*").alias("messages"),
            pct("source_delay_s", 0.5).alias("source_delay_p50_s"),
            pct("broker_s", 0.5).alias("broker_p50_s"),
            pct("broker_s", 0.95).alias("broker_p95_s"),
            pct("pipeline_s", 0.5).alias("pipeline_p50_s"),
            pct("pipeline_s", 0.95).alias("pipeline_p95_s"),
            pct("end_to_end_s", 0.5).alias("end_to_end_p50_s"),
            pct("end_to_end_s", 0.95).alias("end_to_end_p95_s"),
            F.min("kafka_timestamp").alias("first_sent_at"),
            F.max("kafka_timestamp").alias("last_sent_at"),
        )
        .withColumn(
            "send_span_s",
            seconds(F.col("last_sent_at"), F.col("first_sent_at")),
        )
        .withColumn(
            "messages_per_s",
            F.when(F.col("send_span_s") > 0, F.round(F.col("messages") / F.col("send_span_s"), 1)),
        )
    )
