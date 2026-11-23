"""Contract tests for the SCD2 power-plant dimension (P3.4).

The pipeline file imports ``dlt`` (only available in Databricks), so its constants are
read with ``ast`` and checked against the extract, loader and simulator.
"""

import ast
from pathlib import Path

import yaml

from ingestion.batch import mastr_simulator as sim
from ingestion.batch.mastr_load import UNIT_FILES
from src.cdc.mastr_extract import BRONZE_DDL, COLUMNS

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "gold" / "power_plant_pipeline.py"


def constant(name: str):
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {PIPELINE.name}")


def test_pipeline_includes_power_plant_file():
    config = yaml.safe_load((ROOT / "resources" / "bronze.pipeline.yml").read_text())
    paths = [lib["file"]["path"] for lib in config["resources"]["pipelines"]["bronze"]["libraries"]]
    assert "../src/gold/power_plant_pipeline.py" in paths


def test_extract_columns_exist_in_bronze_and_are_not_register_columns():
    for column in constant("EXTRACT_COLUMNS"):
        assert column in BRONZE_DDL
        assert column not in COLUMNS


def test_untracked_columns_are_register_columns():
    assert set(constant("UNTRACKED_COLUMNS")) <= set(COLUMNS)
    assert "unit_id" not in constant("UNTRACKED_COLUMNS")  # the key is always kept
    # Tracked attributes that must open a new version when they change.
    for column in ("status", "net_capacity_kw", "decommissioning_date"):
        assert column not in constant("UNTRACKED_COLUMNS")


def test_quality_rules_match_loader_and_simulator():
    assert set(constant("UNIT_TYPES")) == set(UNIT_FILES)
    assert set(constant("STATUSES")) == {
        sim.IN_OPERATION,
        sim.PLANNED,
        sim.TEMP_STOPPED,
        sim.CLOSED,
    }
