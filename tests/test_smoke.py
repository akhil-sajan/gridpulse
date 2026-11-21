"""Smoke test: the package layout imports cleanly. Real tests arrive from P1.1 onwards."""

import importlib

import pytest

PACKAGES = [
    "ingestion.common",
    "ingestion.producers",
    "ingestion.batch",
    "src.bronze",
    "src.silver",
    "src.ml",
    "src.utils",
    "src.agent.tools",
]


@pytest.mark.parametrize("name", PACKAGES)
def test_package_imports(name: str) -> None:
    assert importlib.import_module(name) is not None
