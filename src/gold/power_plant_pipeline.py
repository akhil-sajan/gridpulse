"""Power-plant dimension with full history (P3.4): SCD Type 2 with AUTO CDC.

Source: ``bronze.mastr_power_units_changes``, the changed rows of the register that the
``extract_mastr`` job task appends from Neon Postgres (watermark CDC, P3.3).

Target: ``gold.dim_power_plant``, one row per *version* of each unit:

* ``__START_AT`` is the ``updated_at`` of the change that created the version,
* ``__END_AT`` is the ``updated_at`` of the next change (NULL = current version).

A new version is only opened when a tracked attribute changes (status, capacity,
location, dates, ...). Pure bookkeeping columns (``mastr_updated_at``, ``change_source``)
and ``updated_at`` itself are kept on the row but don't create versions on their own.

AUTO CDC orders events by ``updated_at``, so late or re-delivered rows land in the right
place. Rows that the extract delivered twice (same ``unit_id`` and ``updated_at``, after a
retried run) are dropped before AUTO CDC.

Note on time: ``__START_AT``/``__END_AT`` are *system time* (when the register record
changed in the operational database). When a plant actually ran is *business time*:
``commissioning_date`` and ``decommissioning_date``. P3.5 uses business time for capacity
history, because the register copy only starts in November 2026.
"""

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("gridpulse.catalog")
SOURCE_TABLE = f"{CATALOG}.bronze.mastr_power_units_changes"
TARGET = f"{CATALOG}.gold.dim_power_plant"

UNIT_TYPES = ("solar", "wind", "combustion", "biomass", "hydro", "nuclear", "storage")
STATUSES = ("In Betrieb", "In Planung", "Vorübergehend stillgelegt", "Endgültig stillgelegt")

# Columns that only describe the extract; not part of the dimension.
EXTRACT_COLUMNS = ["_window_from", "_window_to", "_extract_id", "_extracted_at"]
# Kept on each version, but a change in these alone does not open a new version.
UNTRACKED_COLUMNS = ["mastr_updated_at", "change_source", "updated_at"]

DROP_RULES = {
    "unit_id_present": "unit_id IS NOT NULL",
    "updated_at_present": "updated_at IS NOT NULL",
}
WARN_RULES = {
    "capacity_not_negative": "net_capacity_kw >= 0",
    "known_unit_type": "unit_type IN ({})".format(", ".join(f"'{t}'" for t in UNIT_TYPES)),
    "known_status": "status IN ({})".format(", ".join(f"'{s}'" for s in STATUSES)),
}


@dlt.view(name="mastr_changes_clean", comment="Register changes, re-deliveries removed")
@dlt.expect_all_or_drop(DROP_RULES)
@dlt.expect_all(WARN_RULES)
def mastr_changes_clean():
    return (
        spark.readStream.table(SOURCE_TABLE)
        # A retried extract can deliver the same change twice.
        .dropDuplicates(["unit_id", "updated_at"])
    )


dlt.create_streaming_table(
    name=TARGET,
    comment=(
        "Power plants (MaStR units >= 100 kW), one row per version (SCD Type 2). "
        "__START_AT/__END_AT = when the register record changed; NULL __END_AT = current."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["unit_id"],
)

dlt.apply_changes(
    target=TARGET,
    source="mastr_changes_clean",
    keys=["unit_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    except_column_list=EXTRACT_COLUMNS,
    track_history_except_column_list=UNTRACKED_COLUMNS,
)
