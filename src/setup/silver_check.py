# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · silver checks (P2.1)
# MAGIC Fails the job unless:
# MAGIC 1. every silver key appears exactly once (no leftover partial/final duplicates),
# MAGIC 2. every SMARD 15-minute series with data in 2024 has all 35,136 quarter-hours of
# MAGIC    the UTC year, and hourly prices all 8,784 hours,
# MAGIC 3. every German local day in 2024 has the right number of quarter-hours
# MAGIC    (92 on 31 March, 100 on 27 October, 96 otherwise),
# MAGIC 4. every weather location has all 8,784 hours of 2024.
# MAGIC Quarantine counts are reported but don't fail the job.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")
s = f"{catalog}.silver"
spark.conf.set("spark.sql.session.timeZone", "UTC")
problems = []


def check(label: str, sql: str, expect_zero: bool = True) -> int:
    value = spark.sql(sql).first()[0] or 0
    status = "ok" if (value == 0) == expect_zero else "FAIL"
    print(f"{status:4}  {label}: {value}")
    if status == "FAIL":
        problems.append(f"{label}: {value}")
    return value


# COMMAND ----------

print("1. Keys are unique")
KEYS = {
    "smard_generation": "series_name, resolution, ts_utc",
    "smard_consumption": "series_name, resolution, ts_utc",
    "smard_prices": "series_name, resolution, ts_utc",
    "openmeteo_weather": "location_code, ts_utc",
    "openmeteo_forecast": "location_code, fetched_at, ts_utc",
}
for table, key in KEYS.items():
    check(
        f"duplicate keys in {table}",
        f"SELECT count(*) FROM (SELECT {key} FROM {s}.{table} GROUP BY {key} HAVING count(*) > 1)",
    )

# COMMAND ----------

print("\n2. Complete UTC year 2024")
for table in ("smard_generation", "smard_consumption", "smard_prices"):
    rows = spark.sql(
        f"""
        SELECT series_name, resolution, count(*) AS n
        FROM {s}.{table}
        WHERE year(ts_utc) = 2024
        GROUP BY series_name, resolution
        ORDER BY series_name, resolution
        """
    ).collect()
    for r in rows:
        expected = 35_136 if r.resolution == "quarterhour" else 8_784
        if r.resolution == "quarterhour" and table == "smard_prices":
            continue  # 15-minute day-ahead prices only exist from late 2025
        status = "ok" if r.n == expected else "FAIL"
        print(f"{status:4}  {table}.{r.series_name} ({r.resolution}): {r.n} / {expected}")
        if status == "FAIL":
            problems.append(f"{table}.{r.series_name} {r.resolution}: {r.n} of {expected} in 2024")

# COMMAND ----------

print("\n3. German local days in 2024 have 92 / 96 / 100 quarter-hours")
# Expected length of each local day = UTC seconds between its local midnights / 900.
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW local_days_2024 AS
    SELECT d AS local_date,
           CAST((unix_timestamp(to_utc_timestamp(date_add(d, 1), 'Europe/Berlin'))
                 - unix_timestamp(to_utc_timestamp(d, 'Europe/Berlin'))) / 900 AS INT) AS expected
    FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2024-12-31')) AS d)
    """
)
print(
    spark.sql("SELECT local_date, expected FROM local_days_2024 WHERE expected <> 96 ORDER BY 1")
    .toPandas()
    .to_string(index=False)
)
check(
    "local days with the wrong number of quarter-hours (generation + consumption)",
    f"""
    WITH actual AS (
      SELECT series_name, local_date, count(*) AS n
      FROM (SELECT series_name, local_date FROM {s}.smard_generation
            WHERE resolution = 'quarterhour'
            UNION ALL
            SELECT series_name, local_date FROM {s}.smard_consumption
            WHERE resolution = 'quarterhour')
      WHERE year(local_date) = 2024
      GROUP BY series_name, local_date
    )
    SELECT count(*) FROM actual a JOIN local_days_2024 d USING (local_date) WHERE a.n <> d.expected
    """,
)
check(
    "solar quarter-hours on 2024-03-31 (expect 92)",
    f"""SELECT count(*) - 92 FROM {s}.smard_generation
        WHERE series_name = 'solar' AND resolution = 'quarterhour'
          AND local_date = DATE'2024-03-31'""",
)
check(
    "solar quarter-hours on 2024-10-27 (expect 100)",
    f"""SELECT count(*) - 100 FROM {s}.smard_generation
        WHERE series_name = 'solar' AND resolution = 'quarterhour'
          AND local_date = DATE'2024-10-27'""",
)

# COMMAND ----------

print("\n4. Weather: every location has all hours of 2024")
check(
    "locations with an incomplete 2024",
    f"""SELECT count(*) FROM (
          SELECT location_code FROM {s}.openmeteo_weather WHERE year(ts_utc) = 2024
          GROUP BY location_code HAVING count(*) <> 8784)""",
)
check(
    "weather locations present",
    f"SELECT 16 - count(DISTINCT location_code) FROM {s}.openmeteo_weather",
)

# COMMAND ----------

print("\n5. Row counts and quarantine (informational)")
for table in list(KEYS) + ["smard_quarantine", "openmeteo_quarantine"]:
    n = spark.sql(f"SELECT count(*) FROM {s}.{table}").first()[0]
    print(f"      {table:22} {n:>12,}")
display(
    spark.sql(
        f"""SELECT dataset, series_name, explode(failed_rules) AS rule, count(*) AS rows
            FROM {s}.smard_quarantine GROUP BY ALL ORDER BY rows DESC"""
    )
)

assert not problems, "Silver checks failed:\n" + "\n".join(problems)
print("\nSilver checks passed.")
