"""Contract test: every SMARD generation series has exactly one category in silver."""

import ast
from pathlib import Path

from ingestion.batch.smard import load_config

PIPELINE = Path(__file__).resolve().parents[1] / "src" / "silver" / "silver_pipeline.py"
CATEGORIES = {"renewable", "fossil", "nuclear", "storage"}


def production_types():
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "PRODUCTION_TYPES" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("PRODUCTION_TYPES not found")


def test_every_generation_series_has_one_category():
    rows = production_types()
    by_name = {r[0]: r for r in rows}
    assert len(by_name) == len(rows), "duplicate series in PRODUCTION_TYPES"
    generation = {s.name: s for s in load_config().series if s.dataset == "generation"}
    assert set(by_name) == set(generation)
    for name, series in generation.items():
        assert by_name[name][1] == series.filter_id, name


def test_categories_and_flags_are_consistent():
    for name, _filter, _en, _de, category, renewable, weather in production_types():
        assert category in CATEGORIES, name
        assert renewable == (category == "renewable"), name
        assert not weather or renewable, f"{name}: weather-dependent but not renewable"
