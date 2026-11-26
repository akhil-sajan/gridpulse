# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · prepare catalog (P5.5)
# MAGIC First task of the daily job. Makes sure the target catalog has every schema and the
# MAGIC landing volume. Idempotent: in dev everything exists and nothing changes; for a new
# MAGIC prod catalog it creates them. The catalog itself is created once by hand
# MAGIC (`CREATE CATALOG gridpulse_prod`), as an administrator decision.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")

SCHEMAS = ["bronze", "silver", "gold", "ml", "ops", "staging"]

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.landing")

# Auto Loader fails on a folder that doesn't exist. In prod nothing writes the stream
# folders (the producers feed dev), so create every folder the pipeline reads.
LANDING_FOLDERS = [
    "smard/generation",
    "smard/consumption",
    "smard/prices",
    "open_meteo/archive",
    "open_meteo/forecast",
    "stream/energy_generation_v1",
    "stream/meters_readings_v1",
    "stream/energy_generation_dlq_v1",
    "stream/meters_readings_dlq_v1",
]
for folder in LANDING_FOLDERS:
    dbutils.fs.mkdirs(f"/Volumes/{catalog}/bronze/landing/{folder}")

found = sorted(r[0] for r in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect())
missing = set(SCHEMAS) - set(found)
assert not missing, f"Missing schemas in {catalog}: {sorted(missing)}"
print(f"{catalog}: schemas {', '.join(SCHEMAS)} and volume bronze.landing ready.")
print(f"Landing folders: {len(LANDING_FOLDERS)} present.")
