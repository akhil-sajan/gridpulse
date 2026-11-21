# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · bundle check (P0.4)
# MAGIC Deployed by the Asset Bundle. Confirms the job runs on serverless compute
# MAGIC and can see this target's Unity Catalog catalog.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")

schemas = sorted(r[0] for r in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect())
expected = {"bronze", "silver", "gold", "ml", "ops"}
missing = expected - set(schemas)

print(f"Catalog: {catalog}")
print(f"Schemas: {', '.join(schemas)}")
assert not missing, f"Missing schemas in {catalog}: {sorted(missing)}"
print("Bundle check passed.")
