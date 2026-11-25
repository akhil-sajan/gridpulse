# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · freshness and volume checks (P5.2)
# MAGIC Runs after the pipeline in the daily job. Every check is appended to
# MAGIC `ops.data_quality_checks`; if any check fails, this task fails and the job sends
# MAGIC its failure email.
# MAGIC
# MAGIC * **Freshness:** age of the newest data. Batch sources: when the newest file was
# MAGIC   landed. Live stream: the newest `produced_at`, i.e. whether the producer is alive.
# MAGIC * **Volume:** rows loaded in the last 24 hours vs the average of the 7 days before.

# COMMAND ----------

import sys

dbutils.widgets.text("catalog", "gridpulse_dev")
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("batch_max_age_hours", "26")
dbutils.widgets.text("stream_max_age_hours", "12")
catalog = dbutils.widgets.get("catalog")
repo_root = dbutils.widgets.get("repo_root")
batch_max = float(dbutils.widgets.get("batch_max_age_hours"))
stream_max = float(dbutils.widgets.get("stream_max_age_hours"))
if repo_root and repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.utils import dq  # noqa: E402

# table -> (timestamp column that says when data arrived, freshness limit in hours)
FRESHNESS = {
    "bronze.smard_generation": ("_file_modified_at", batch_max),
    "bronze.smard_prices": ("_file_modified_at", batch_max),
    "bronze.openmeteo_archive": ("_file_modified_at", batch_max),
    "bronze.openmeteo_forecast": ("_file_modified_at", batch_max),
    "bronze.mastr_power_units_changes": ("_extracted_at", batch_max),
    # Live stream: when the producer last sent anything (either transport).
    "bronze.kafka_energy_generation": (
        "cast(get_json_object(value, '$.produced_at') AS timestamp)",
        stream_max,
    ),
}
# table -> column with the load time of each row
VOLUME = {
    "silver.smard_generation": "_file_modified_at",
    "silver.smard_consumption": "_file_modified_at",
    "silver.smard_prices": "_file_modified_at",
    "silver.openmeteo_weather": "_file_modified_at",
    "bronze.kafka_energy_generation": "_ingested_at",
}

# COMMAND ----------

checks = []
for table, (ts_expr, limit) in FRESHNESS.items():
    [[age]] = spark.sql(
        f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max({ts_expr}))) / 3600 "
        f"FROM {catalog}.{table}"
    ).collect()
    checks.append(dq.freshness(table, age, limit))

for table, ts in VOLUME.items():
    rows = spark.sql(
        f"""
        SELECT CAST(floor((unix_timestamp(current_timestamp()) - unix_timestamp({ts})) / 86400)
                    AS INT) AS days_ago,
               count(*) AS n
        FROM {catalog}.{table}
        WHERE {ts} >= current_timestamp() - INTERVAL 8 DAYS
        GROUP BY 1
        """
    ).collect()
    per_day = {r.days_ago: r.n for r in rows}
    checks.append(
        dq.volume(table, per_day.get(0, 0), [per_day[d] for d in range(1, 8) if d in per_day])
    )

# COMMAND ----------

result = spark.createDataFrame(
    [c.as_row() for c in checks],
    "check_type STRING, table_name STRING, measured DOUBLE, threshold DOUBLE, "
    "status STRING, detail STRING",
).selectExpr("current_timestamp() AS checked_at", "*")
result.write.mode("append").saveAsTable(f"{catalog}.ops.data_quality_checks")

print(f"{'status':6} {'type':9} {'table':36} detail")
for c in checks:
    print(f"{c.status:6} {c.check_type:9} {c.table_name:36} {c.detail}")

failed = dq.failures(checks)
assert not failed, "Data quality checks failed:\n" + "\n".join(
    f"{c.check_type} {c.table_name}: {c.detail}" for c in failed
)
print(f"\nAll {len(checks)} checks passed or warned; none failed.")
