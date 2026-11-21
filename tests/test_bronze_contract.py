"""Contract test: file names written by the ingesters must match the bronze patterns.

The bronze pipeline runs inside Databricks and can't be imported here, so the regex
constants are read straight from its source file.
"""

import ast
import re
from datetime import date
from pathlib import Path

import pytest

from ingestion.batch.open_meteo import Location, Month, archive_file_name
from ingestion.batch.smard import Series, file_name

PIPELINE = Path(__file__).resolve().parents[1] / "src" / "bronze" / "bronze_pipeline.py"


def pipeline_constant(name: str) -> str:
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {PIPELINE}")


SMARD_NAME = re.compile(pipeline_constant("SMARD_NAME"))
ARCHIVE_NAME = re.compile(pipeline_constant("ARCHIVE_NAME"))
FORECAST_NAME = re.compile(pipeline_constant("FORECAST_NAME"))

HE = Location("de_he", "Hessen", "Wiesbaden", 50.0, 8.2)
JAN = Month(date(2024, 1, 1), date(2024, 1, 31))


@pytest.mark.parametrize(
    ("series", "resolution", "partial"),
    [
        (Series("generation", "solar", 4068, "DE", ("quarterhour",)), "quarterhour", None),
        (
            Series("generation", "wind_onshore", 4067, "DE", ("quarterhour",)),
            "quarterhour",
            "20261121T1608Z",
        ),
        (Series("prices", "day_ahead", 4169, "DE-LU", ("hour",)), "hour", None),
    ],
)
def test_smard_names_match_bronze_pattern(series, resolution, partial):
    name = file_name(series, resolution, 1704063600000, partial)
    m = SMARD_NAME.match(name)
    assert m, name
    assert m.group(1) == series.name
    assert int(m.group(2)) == series.filter_id
    assert m.group(3) == series.region
    assert m.group(4) == resolution
    assert m.group(5) == "1704063600000"
    assert m.group(6) == partial


@pytest.mark.parametrize("partial", [None, "20240205T0600Z"])
def test_archive_names_match_bronze_pattern(partial):
    m = ARCHIVE_NAME.match(archive_file_name(HE, JAN, partial))
    assert m and m.group(1) == "de_he" and m.group(2) == "2024-01" and m.group(3) == partial


def test_forecast_names_match_bronze_pattern():
    # Same format as ingestion.batch.open_meteo.run_forecast
    m = FORECAST_NAME.match(f"{HE.code}_forecast_20261121T1746Z.json")
    assert m and m.group(1) == "de_he" and m.group(2) == "20261121T1746Z"
