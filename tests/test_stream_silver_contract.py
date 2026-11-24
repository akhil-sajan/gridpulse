"""Contract tests for streaming silver (P4.5): parse schemas match the producers' JSON Schemas."""

import ast
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "silver" / "stream_silver.py"
SCHEMAS = ROOT / "ingestion" / "producers" / "schemas"


def constant(name):
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(name)


def fields(ddl: str) -> set[str]:
    return {part.split()[0] for part in ddl.replace("\n", " ").split(",") if part.strip()}


def schema(name: str) -> dict:
    return json.loads((SCHEMAS / name).read_text(encoding="utf-8"))


def test_envelope_matches_both_contracts():
    for file in ("energy_generation.v1.json", "meter_readings.v1.json"):
        assert fields(constant("ENVELOPE")) == set(schema(file)["required"])


def test_payload_schemas_match_contracts():
    gen = schema("energy_generation.v1.json")["properties"]["payload"]["properties"]
    met = schema("meter_readings.v1.json")["properties"]["payload"]["properties"]
    assert fields(constant("GENERATION_PAYLOAD")) == set(gen)
    assert fields(constant("METER_PAYLOAD")) == set(met)


def test_pipeline_includes_stream_silver():
    pipeline = yaml.safe_load((ROOT / "resources" / "bronze.pipeline.yml").read_text())
    paths = [
        lib["file"]["path"] for lib in pipeline["resources"]["pipelines"]["bronze"]["libraries"]
    ]
    assert "../src/silver/stream_silver.py" in paths
