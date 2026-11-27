# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · freshness and volume checks (P5.2)
# MAGIC Runs after the pipeline in the daily job. Every check is appended to
# MAGIC `ops.data_quality_checks`; if any check fails, this task fails and the job sends
# MAGIC its failure email.
# MAGIC
# MAGIC * **Freshness:** age of the newest data. Batch sources: when the newest file was
# MAGIC   landed. Live stream: the newest `produced_at`, i.e. whether the producer is alive.
# MAGIC * **Volume:** rows of the last complete data day vs the median of the 7 days before
# MAGIC   it (per data day, not load time, so backfills don't distort the baseline).

# COMMAND ----------

import sys

dbutils.widgets.text("catalog", "gridpulse_dev")
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("batch_max_age_hours", "26")
dbutils.widgets.text("stream_max_age_hours", "12")
# P5.5: prod has no live stream (the producers feed dev), so it skips the stream checks.
dbutils.widgets.text("check_stream", "true")
catalog = dbutils.widgets.get("catalog")
repo_root = dbutils.widgets.get("repo_root")
batch_max = float(dbutils.widgets.get("batch_max_age_hours"))
stream_max = float(dbutils.widgets.get("stream_max_age_hours"))
check_stream = dbutils.widgets.get("check_stream").strip().lower() == "true"
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
# table -> expression giving the data day of each row (the day the values describe).
VOLUME = {
    "silver.smard_generation": "local_date",
    "silver.smard_consumption": "local_date",
    "silver.smard_prices": "local_date",
    "silver.openmeteo_weather": "local_date",
    # Deduplicated live stream (bronze holds every re-send, so it can't be counted per day).
    "silver.stream_energy_generation": "to_date(event_ts_utc)",
}
if not check_stream:
    FRESHNESS.pop("bronze.kafka_energy_generation")
    VOLUME.pop("silver.stream_energy_generation")

# COMMAND ----------

checks = []
for table, (ts_expr, limit) in FRESHNESS.items():
    [[age]] = spark.sql(
        f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max({ts_expr}))) / 3600 "
        f"FROM {catalog}.{table}"
    ).collect()
    checks.append(dq.freshness(table, age, limit))

for table, day_expr in VOLUME.items():
    rows = spark.sql(
        f"""
        WITH per_day AS (
          SELECT {day_expr} AS d, count(*) AS n FROM {catalog}.{table} GROUP BY 1
        ),
        -- The newest data day is usually still filling; the one before is complete.
        target AS (SELECT date_sub(max(d), 1) AS d FROM per_day)
        SELECT p.d, p.n, datediff(t.d, p.d) AS days_before
        FROM per_day AS p CROSS JOIN target AS t
        WHERE p.d BETWEEN date_sub(t.d, 7) AND t.d
        """
    ).collect()
    by_offset = {r.days_before: r.n for r in rows}
    day = next((str(r.d) for r in rows if r.days_before == 0), "last day")
    checks.append(
        dq.volume(
            table,
            by_offset.get(0, 0),
            [by_offset.get(k, 0) for k in range(1, 8) if k in by_offset],
            day=day,
        )
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
