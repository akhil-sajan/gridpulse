"""Bronze tables for the event streams (P4.4, P4.7).

Each stream has one bronze table that keeps every message exactly as it arrived: the raw
``value`` and ``key`` as text, ``topic``, where it sat in the Kafka log (``partition``,
``offset``), ``kafka_timestamp`` (broker time) and when bronze stored it (``_ingested_at``),
plus ``transport``: how the message came in.

Two transports can fill the same table, as separate append flows:

* **kafka** (P4.4): Structured Streaming from Redpanda (``format("kafka")``), SASL_SSL +
  SCRAM, credentials from the secret scope ``gridpulse``.
* **file** (P4.7, fallback after the Redpanda trial): the producers write micro-batch
  JSON-lines files to ``<landing>/stream/<topic>/``; Auto Loader reads them. Files have
  no partition or offset; ``kafka_timestamp`` is the file's arrival time in the volume.

Silver reads the table, not the transport, so switching transport changes nothing
downstream. ``gridpulse.kafka_enabled = false`` removes the Kafka flows: the pipeline no
longer contacts Redpanda, and the rows that came through Kafka stay in the table.

The Kafka flows keep the names the tables had before (``kafka_energy_generation`` ...),
so their streaming checkpoints, i.e. their read positions in the log, carry over.

Tables (catalog from the bundle target, schema ``bronze``):
    kafka_energy_generation   <- energy.generation.v1
    kafka_meter_readings      <- meters.readings.v1
    kafka_dlq                 <- energy.generation.dlq.v1, meters.readings.dlq.v1
(The names stay for continuity; they mean "event stream", whatever the transport.)
"""

import dlt
from pyspark.sql import functions as F

SECRET_SCOPE = spark.conf.get("gridpulse.secret_scope", "gridpulse")
KAFKA_ENABLED = spark.conf.get("gridpulse.kafka_enabled", "true").lower() == "true"
LANDING_ROOT = spark.conf.get("gridpulse.landing_root")

TOPICS = {
    "kafka_energy_generation": "energy.generation.v1",
    "kafka_meter_readings": "meters.readings.v1",
}
DLQ_TOPICS = "energy.generation.dlq.v1,meters.readings.dlq.v1"
FILE_LINE_SCHEMA = "topic STRING, key STRING, value STRING"

JAAS = (
    "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    'username="{username}" password="{password}";'
)


def kafka_options() -> dict:
    """Spark Kafka source options for Redpanda Serverless (SASL_SSL + SCRAM-SHA-256)."""

    def secret(key: str) -> str:
        return dbutils.secrets.get(SECRET_SCOPE, key).strip()

    return {
        "kafka.bootstrap.servers": secret("kafka_bootstrap"),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-256",
        "kafka.sasl.jaas.config": JAAS.format(
            username=secret("kafka_username"), password=secret("kafka_password")
        ),
        # First run reads the whole retained log; later runs continue from the checkpoint.
        "startingOffsets": "earliest",
        # Fail loudly instead of silently skipping messages that expired before we read them.
        "failOnDataLoss": "true",
    }


def from_kafka(topics: str):
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", topics)
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.lit("kafka").alias("transport"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


def file_entity(topic: str) -> str:
    """Folder name the FileSink uses: energy.generation.v1 -> energy_generation_v1."""
    return topic.replace(".", "_").replace("-", "_")


def from_files(topics: str):
    paths = ",".join(f"{LANDING_ROOT}/stream/{file_entity(t)}" for t in topics.split(","))
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(FILE_LINE_SCHEMA)
    )
    streams = [reader.load(path) for path in paths.split(",")]
    df = streams[0]
    for other in streams[1:]:
        df = df.unionByName(other)
    return df.select(
        F.col("topic"),
        F.lit(None).cast("int").alias("partition"),
        F.lit(None).cast("bigint").alias("offset"),
        F.col("_metadata.file_modification_time").alias("kafka_timestamp"),
        F.col("key"),
        F.col("value"),
        F.lit("file").alias("transport"),
        F.current_timestamp().alias("_ingested_at"),
    )


def define_stream_table(table: str, topics: str, comment: str) -> None:
    dlt.create_streaming_table(
        name=table,
        comment=comment,
        table_properties={"quality": "bronze"},
        cluster_by=["partition", "offset"],
        expect_all={"value_present": "value IS NOT NULL"},
    )

    if KAFKA_ENABLED:

        @dlt.append_flow(target=table, name=table)
        def _kafka():
            return from_kafka(topics)

    @dlt.append_flow(target=table, name=f"{table}_files")
    def _files():
        return from_files(topics)


for _table, _topic in TOPICS.items():
    define_stream_table(
        _table, _topic, f"Raw messages of stream {_topic}, one row per message, unchanged."
    )

define_stream_table(
    "kafka_dlq",
    DLQ_TOPICS,
    "Events the producers rejected against their JSON Schema (both DLQ streams).",
)
