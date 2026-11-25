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


def test_smoke_main_lands_file_once(monkeypatch, tmp_path, capsys):
    """The P1.1 smoke script, without network: fetch -> land -> confirm."""
    from ingestion.common import smoke

    monkeypatch.setenv("GRIDPULSE_LANDING_BACKEND", "local")
    monkeypatch.setenv("GRIDPULSE_LOCAL_LANDING_DIR", str(tmp_path))
    monkeypatch.setattr(smoke.HttpClient, "get_json", lambda self, url, params=None: {"ok": 1})

    assert smoke.main() == 0
    assert "Smoke test PASSED" in capsys.readouterr().out
    landed = list(tmp_path.rglob("public_power_*.json"))
    assert len(landed) == 1 and landed[0].read_text() == '{"ok": 1}'
