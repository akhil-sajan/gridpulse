"""Bronze streaming tables from Kafka / Redpanda (P4.4).

Reads the GridPulse topics with Structured Streaming (``format("kafka")``) and keeps every
message exactly as it arrived: the raw ``value`` and ``key`` as text, plus where it sits
in the log (``topic``, ``partition``, ``offset``), the broker's ``kafka_timestamp`` and when
bronze stored it (``_ingested_at``). Parsing, validation and deduplication happen in
silver (P4.5), so a bad message can always be replayed from bronze.

The pipeline is triggered, not continuous: each update reads everything that arrived
since the last one (offsets are kept in the streaming checkpoint) and stops. Topics keep
7 days of data, so a daily refresh never loses messages.

Tables (catalog from the bundle target, schema ``bronze``):
    kafka_energy_generation   <- energy.generation.v1
    kafka_meter_readings      <- meters.readings.v1
    kafka_dlq                 <- energy.generation.dlq.v1, meters.readings.dlq.v1

Connection: bootstrap server, username and password come from the secret scope
``gridpulse`` (keys ``kafka_bootstrap``, ``kafka_username``, ``kafka_password``).
"""

import dlt
from pyspark.sql import functions as F

SECRET_SCOPE = spark.conf.get("gridpulse.secret_scope", "gridpulse")

TOPICS = {
    "kafka_energy_generation": "energy.generation.v1",
    "kafka_meter_readings": "meters.readings.v1",
}
DLQ_TOPICS = "energy.generation.dlq.v1,meters.readings.dlq.v1"

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


def raw(stream):
    """Keep the message as text plus its position in the log."""
    return stream.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.current_timestamp().alias("_ingested_at"),
    )


def define_topic_table(table: str, topic: str) -> None:
    @dlt.table(
        name=table,
        comment=f"Raw messages from Kafka topic {topic}, one row per message, unchanged.",
        table_properties={"quality": "bronze"},
        cluster_by=["partition", "offset"],
    )
    @dlt.expect("value_present", "value IS NOT NULL")
    def _table():
        return raw(
            spark.readStream.format("kafka")
            .options(**kafka_options())
            .option("subscribe", topic)
            .load()
        )


for _table, _topic in TOPICS.items():
    define_topic_table(_table, _topic)


@dlt.table(
    name="kafka_dlq",
    comment="Events the producers rejected against their JSON Schema (both DLQ topics).",
    table_properties={"quality": "bronze"},
)
def kafka_dlq():
    return raw(
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", DLQ_TOPICS)
        .load()
    )
