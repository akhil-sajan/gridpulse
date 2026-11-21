# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · bronze reconciliation (P1.5)
# MAGIC Bronze stores one row per landed file, so every table must hold exactly as many
# MAGIC distinct source files as its landing folder contains. Fails the job on a mismatch
# MAGIC or on file names the pipeline could not parse.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")
root = f"/Volumes/{catalog}/bronze/landing"

CHECKS = {
    "smard_generation": ("smard/generation", "series_name"),
    "smard_consumption": ("smard/consumption", "series_name"),
    "smard_prices": ("smard/prices", "series_name"),
    "openmeteo_archive": ("open_meteo/archive", "location_code"),
    "openmeteo_forecast": ("open_meteo/forecast", "location_code"),
}


def count_files(path: str) -> int:
    total, pending = 0, [path]
    while pending:
        for entry in dbutils.fs.ls(pending.pop()):
            if entry.isDir():
                pending.append(entry.path)
            elif entry.name.endswith(".json"):
                total += 1
    return total


# COMMAND ----------

problems, rows = [], []
for table, (folder, key_col) in CHECKS.items():
    files = count_files(f"{root}/{folder}")
    stats = spark.sql(
        f"""
        SELECT count(*) AS row_count,
               count(DISTINCT _source_file) AS file_count,
               count_if({key_col} = '') AS unparsed,
               count_if(_rescued_data IS NOT NULL) AS rescued
        FROM {catalog}.bronze.{table}
        """
    ).first()
    rows.append((table, files, stats.file_count, stats.row_count, stats.unparsed, stats.rescued))
    if stats.file_count != files or stats.row_count != files:
        problems.append(f"{table}: {files} files landed, {stats.file_count} in bronze")
    if stats.unparsed:
        problems.append(f"{table}: {stats.unparsed} file names not recognised")

print(f"{'table':22} {'landed':>8} {'bronze':>8} {'rows':>8} {'unparsed':>9} {'rescued':>8}")
for r in rows:
    print(f"{r[0]:22} {r[1]:>8} {r[2]:>8} {r[3]:>8} {r[4]:>9} {r[5]:>8}")

assert not problems, "Bronze reconciliation failed:\n" + "\n".join(problems)
print("\nBronze reconciliation passed.")
