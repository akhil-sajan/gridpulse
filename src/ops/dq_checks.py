"""Hard data-quality gate (P5.1): keys must be unique in silver and gold.

Row-level problems (a negative value, a field that didn't parse) are expected now and
then: those rows are dropped and kept in quarantine tables, and the pipeline carries on.
A duplicate key is different: it means a merge or deduplication step is broken, and
every report built on the table would double-count. That must never happen, so it uses
``expect_all_or_fail``: the pipeline update fails and the job sends its failure email.

``ops.dq_key_uniqueness`` has one row per checked table with the number of keys that
appear more than once (expected: 0 everywhere).
"""

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("gridpulse.catalog")

# table -> (key columns, optional filter)
KEYS = {
    "silver.smard_generation": (["series_name", "resolution", "ts_utc"], None),
    "silver.smard_consumption": (["series_name", "resolution", "ts_utc"], None),
    "silver.smard_prices": (["series_name", "resolution", "ts_utc"], None),
    "silver.openmeteo_weather": (["location_code", "ts_utc"], None),
    "silver.stream_meter_readings": (["event_id"], None),
    "silver.stream_energy_generation": (["production_type_slug", "event_ts_utc"], None),
    "gold.fact_meter_consumption_30min": (["window_start_utc"], None),
    # SCD2: many versions per unit, but only one current version.
    "gold.dim_power_plant": (["unit_id"], "__END_AT IS NULL"),
}


def duplicates(table: str, keys: list[str], where: str | None):
    df = spark.read.table(f"{CATALOG}.{table}")
    if where:
        df = df.where(where)
    dup_keys = df.groupBy(*keys).count().where("count > 1")
    return dup_keys.agg(F.count("*").alias("duplicate_keys")).select(
        F.lit(table).alias("table_name"),
        F.lit(", ".join(keys)).alias("key_columns"),
        F.col("duplicate_keys"),
        F.current_timestamp().alias("checked_at"),
    )


@dlt.table(
    name=f"{CATALOG}.ops.dq_key_uniqueness",
    comment="Duplicate keys per silver/gold table; the pipeline fails if any is above 0.",
    table_properties={"quality": "ops"},
)
@dlt.expect_all_or_fail({"keys_unique": "duplicate_keys = 0"})
def dq_key_uniqueness():
    frames = [duplicates(t, keys, where) for t, (keys, where) in KEYS.items()]
    out = frames[0]
    for other in frames[1:]:
        out = out.unionByName(other)
    return out
