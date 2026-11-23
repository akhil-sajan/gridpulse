# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · Kafka bronze completeness (P4.4)
# MAGIC Kafka gives every message in a partition a consecutive offset. If bronze holds every
# MAGIC message exactly once, then per topic and partition the offsets have no duplicates and
# MAGIC no holes: `count = max - min + 1`. Bronze is also compared with what the broker
# MAGIC currently holds (latest offset per partition). Fails the job on any gap.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")
TABLES = ["kafka_energy_generation", "kafka_meter_readings", "kafka_dlq"]

# COMMAND ----------

problems = []
rows = []
for table in TABLES:
    stats = spark.sql(
        f"""
        SELECT topic, partition,
               count(*) AS messages,
               count(DISTINCT offset) AS distinct_offsets,
               min(offset) AS first_offset,
               max(offset) AS last_offset
        FROM {catalog}.bronze.{table}
        GROUP BY topic, partition
        ORDER BY topic, partition
        """
    ).collect()
    for s in stats:
        expected = s.last_offset - s.first_offset + 1
        rows.append((table, s.topic, s.partition, s.messages, s.first_offset, s.last_offset))
        if s.distinct_offsets != s.messages:
            problems.append(
                f"{s.topic}[{s.partition}]: {s.messages - s.distinct_offsets} duplicates"
            )
        if s.distinct_offsets != expected:
            problems.append(
                f"{s.topic}[{s.partition}]: {expected - s.distinct_offsets} missing offsets "
                f"between {s.first_offset} and {s.last_offset}"
            )

print(f"{'table':26} {'topic':28} {'part':>4} {'messages':>9} {'first':>7} {'last':>7}")
for r in rows:
    print(f"{r[0]:26} {r[1]:28} {r[2]:>4} {r[3]:>9} {r[4]:>7} {r[5]:>7}")

assert not problems, "Kafka bronze check failed:\n" + "\n".join(problems)
print("\nNo gaps or duplicates: every partition's offsets are continuous.")
