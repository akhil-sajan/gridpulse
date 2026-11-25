"""Contract test (P5.1): the uniqueness gate checks the same keys the merges use."""

import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def keys_constant():
    tree = ast.parse((ROOT / "src" / "ops" / "dq_checks.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "KEYS" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("KEYS")


def merge_keys(path: str) -> list[list[str]]:
    text = (ROOT / path).read_text(encoding="utf-8")
    return [ast.literal_eval(m) for m in re.findall(r"keys=(\[[^\]]*\])", text)]


def test_every_auto_cdc_key_is_checked():
    checked = {tuple(keys) for keys, _ in keys_constant().values()}
    for path in (
        "src/silver/silver_pipeline.py",
        "src/silver/stream_silver.py",
        "src/gold/power_plant_pipeline.py",
    ):
        for keys in merge_keys(path):
            assert tuple(keys) in checked, (path, keys)


def test_dq_checks_in_pipeline():
    pipeline = yaml.safe_load((ROOT / "resources" / "bronze.pipeline.yml").read_text())
    paths = [
        lib["file"]["path"] for lib in pipeline["resources"]["pipelines"]["bronze"]["libraries"]
    ]
    assert "../src/ops/dq_checks.py" in paths
