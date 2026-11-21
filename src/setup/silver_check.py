# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse · silver checks (P2.1)
# MAGIC Fails the job unless:
# MAGIC 1. every silver key appears exactly once (no leftover partial/final duplicates),
# MAGIC 2. no SMARD series has gaps in 2024 between its first and last value (a series that
# MAGIC    runs all year has all 35,136 quarter-hours / 8,784 hours; a series that ended,
# MAGIC    like nuclear, is reported and checked up to its last value),
# MAGIC 3. every German local day in 2024 has the right number of quarter-hours
# MAGIC    (92 on 31 March, 100 on 27 October, 96 otherwise), ignoring each series' partial
# MAGIC    first and last day,
# MAGIC 4. every weather location has all 8,784 hours of 2024,
# MAGIC 5. every generation series has a category, and the national weather has one row
# MAGIC    per hour built from all 16 locations.
# MAGIC Quarantine counts are reported but don't fail the job.

# COMMAND ----------

dbutils.widgets.text("catalog", "gridpulse_dev")
catalog = dbutils.widgets.get("catalog")
s = f"{catalog}.silver"
spark.conf.set("spark.sql.session.timeZone", "UTC")
problems = []


def check(label: str, sql: str) -> int:
    """Run a query returning one number; anything other than 0 is a failure."""
    value = spark.sql(sql).first()[0] or 0
    status = "ok" if value == 0 else "FAIL"
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

print("\n2. No gaps in 2024 between each series' first and last value")
for table in ("smard_generation", "smard_consumption", "smard_prices"):
    rows = spark.sql(
        f"""
        WITH bounds AS (
          SELECT series_name, resolution, min(ts_utc) AS first_ts, max(ts_utc) AS last_ts,
                 CASE resolution WHEN 'quarterhour' THEN 900 ELSE 3600 END AS step
          FROM {s}.{table}
          GROUP BY series_name, resolution
        ),
        win AS (
          SELECT *,
                 greatest(first_ts, TIMESTAMP'2024-01-01 00:00:00') AS win_start,
                 least(last_ts, CASE resolution
                                  WHEN 'quarterhour' THEN TIMESTAMP'2024-12-31 23:45:00'
                                  ELSE TIMESTAMP'2024-12-31 23:00:00' END) AS win_end
          FROM bounds
        )
        SELECT w.series_name, w.resolution, w.first_ts, w.last_ts,
               CAST((unix_timestamp(w.win_end) - unix_timestamp(w.win_start)) / w.step + 1
                    AS BIGINT) AS expected,
               count(t.ts_utc) AS n
        FROM win w
        LEFT JOIN {s}.{table} t
          ON t.series_name = w.series_name
         AND t.resolution = w.resolution
         AND t.ts_utc BETWEEN w.win_start AND w.win_end
        WHERE w.win_start <= w.win_end
        GROUP BY ALL
        ORDER BY w.series_name, w.resolution
        """
    ).collect()
    for r in rows:
        note = ""
        if r.first_ts.year == 2024 and (r.first_ts.month, r.first_ts.day) != (1, 1):
            note += f"  (series starts {r.first_ts:%Y-%m-%d %H:%M} UTC)"
        if r.last_ts.year == 2024 and (r.last_ts.month, r.last_ts.day) != (12, 31):
            note += f"  (series ends {r.last_ts:%Y-%m-%d %H:%M} UTC)"
        status = "ok" if r.n == r.expected else "FAIL"
        print(f"{status:4}  {table}.{r.series_name} ({r.resolution}): {r.n} / {r.expected}{note}")
        if status == "FAIL":
            problems.append(f"{table}.{r.series_name} {r.resolution}: {r.expected - r.n} missing")

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
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW quarterhour_points AS
    SELECT series_name, local_date FROM {s}.smard_generation WHERE resolution = 'quarterhour'
    UNION ALL
    SELECT series_name, local_date FROM {s}.smard_consumption WHERE resolution = 'quarterhour'
    """
)
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_edges AS
    SELECT series_name, min(local_date) AS first_day, max(local_date) AS last_day
    FROM quarterhour_points GROUP BY series_name
    """
)
print("Switch days:")
for r in spark.sql("SELECT * FROM local_days_2024 WHERE expected <> 96 ORDER BY 1").collect():
    print(f"      {r.local_date}: {r.expected} quarter-hours")
check(
    "local days with the wrong number of quarter-hours (generation + consumption)",
    """
    WITH actual AS (
      SELECT series_name, local_date, count(*) AS n
      FROM quarterhour_points
      WHERE year(local_date) = 2024
      GROUP BY series_name, local_date
    )
    SELECT count(*)
    FROM actual a
    JOIN local_days_2024 d USING (local_date)
    JOIN series_edges e USING (series_name)
    WHERE a.n <> d.expected AND a.local_date NOT IN (e.first_day, e.last_day)
    """,
)
for day, expected in (("2024-03-31", 92), ("2024-10-27", 100)):
    check(
        f"solar quarter-hours on {day} minus {expected}",
        f"""SELECT count(*) - {expected} FROM {s}.smard_generation
            WHERE series_name = 'solar' AND resolution = 'quarterhour'
              AND local_date = DATE'{day}'""",
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
    "weather locations missing (of 16)",
    f"SELECT 16 - count(DISTINCT location_code) FROM {s}.openmeteo_weather",
)

# COMMAND ----------

print("\n5. Reference data and national weather (P2.2, P2.3)")
check(
    "generation series without a category",
    f"""SELECT count(DISTINCT g.series_name) FROM {s}.smard_generation g
        LEFT ANTI JOIN {s}.ref_production_type r ON g.series_name = r.series_name""",
)
check(
    "categories outside renewable/fossil/nuclear/storage",
    f"""SELECT count(*) FROM {s}.ref_production_type
        WHERE category NOT IN ('renewable', 'fossil', 'nuclear', 'storage')""",
)
check(
    "duplicate hours in weather_national_hourly",
    f"""SELECT count(*) - count(DISTINCT ts_utc) FROM {s}.weather_national_hourly""",
)
check(
    "2024 hours in weather_national_hourly not built from all 16 locations",
    f"""SELECT 8784 - count_if(n_locations = 16) FROM {s}.weather_national_hourly
        WHERE year(ts_utc) = 2024""",
)

# COMMAND ----------

print("\n6. Row counts and quarantine (informational)")
for table in [
    *KEYS,
    "ref_production_type",
    "weather_national_hourly",
    "smard_quarantine",
    "openmeteo_quarantine",
]:
    n = spark.sql(f"SELECT count(*) FROM {s}.{table}").first()[0]
    print(f"      {table:22} {n:>12,}")
for r in spark.sql(
    f"""SELECT dataset, series_name, rule, count(*) AS n
        FROM (SELECT dataset, series_name, explode(failed_rules) AS rule FROM {s}.smard_quarantine)
        GROUP BY ALL ORDER BY n DESC LIMIT 20"""
).collect():
    print(f"      quarantine: {r.dataset}.{r.series_name} broke {r.rule}: {r.n} rows")

assert not problems, "Silver checks failed:\n" + "\n".join(problems)
print("\nSilver checks passed.")
