"""Contract tests for the Kafka bronze tables (P4.4): topics match the producers."""

import ast
from pathlib import Path

import yaml

from ingestion.producers.events import TOPICS, dlq_topic

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "bronze" / "kafka_bronze.py"


def constant(name):
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(name)


def test_every_producer_topic_has_a_bronze_table():
    assert set(constant("TOPICS").values()) == set(TOPICS)


def test_dlq_table_reads_both_dead_letter_topics():
    assert set(constant("DLQ_TOPICS").split(",")) == {dlq_topic(t) for t in TOPICS}


def test_pipeline_includes_kafka_file_and_job_checks_offsets():
    pipeline = yaml.safe_load((ROOT / "resources" / "bronze.pipeline.yml").read_text())
    paths = [
        lib["file"]["path"] for lib in pipeline["resources"]["pipelines"]["bronze"]["libraries"]
    ]
    assert "../src/bronze/kafka_bronze.py" in paths
    job = yaml.safe_load((ROOT / "resources" / "refresh.job.yml").read_text())
    tasks = {t["task_key"] for t in job["resources"]["jobs"]["bronze_refresh"]["tasks"]}
    assert "check_kafka_bronze" in tasks
