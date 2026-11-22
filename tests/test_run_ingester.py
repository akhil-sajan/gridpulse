import os
import sys
import types

import pytest

from src.jobs import run_ingester


@pytest.fixture
def fake_module(monkeypatch):
    calls = {}
    module = types.ModuleType("ingestion.batch.smard")

    def main(args):
        calls["args"] = args
        calls["env"] = os.environ.get("GRIDPULSE_ENV")
        calls["backend"] = os.environ.get("GRIDPULSE_LANDING_BACKEND")
        return calls.get("return_code", 0)

    module.main = main
    monkeypatch.setitem(sys.modules, "ingestion.batch.smard", module)
    monkeypatch.delenv("GRIDPULSE_LANDING_BACKEND", raising=False)
    return calls


def test_sets_env_path_and_passes_args(fake_module, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "path", list(sys.path))
    code = run_ingester.run([str(tmp_path), "prod", "ingestion.batch.smard", "--workers", "4"])
    assert code == 0
    assert fake_module == {"args": ["--workers", "4"], "env": "prod", "backend": "volume"}
    assert sys.path[0] == str(tmp_path)


def test_returns_ingester_exit_code(fake_module):
    fake_module["return_code"] = 1
    assert run_ingester.run(["/repo", "dev", "ingestion.batch.smard"]) == 1


def test_rejects_unknown_modules_and_missing_args():
    with pytest.raises(SystemExit, match="unknown ingester module"):
        run_ingester.run(["/repo", "dev", "os"])
    with pytest.raises(SystemExit, match="usage"):
        run_ingester.run(["/repo", "dev"])
