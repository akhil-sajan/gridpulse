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


def test_simulator_gets_dsn_from_secret(monkeypatch, tmp_path):
    import types

    from src.jobs import run_ingester

    calls = {}
    module = types.ModuleType("ingestion.batch.mastr_simulator")

    def fake_main(args):
        import os

        calls["dsn"] = os.environ.get("GRIDPULSE_PG_DSN")
        return 0

    module.main = fake_main
    monkeypatch.setitem(__import__("sys").modules, "ingestion.batch.mastr_simulator", module)
    monkeypatch.delenv("GRIDPULSE_PG_DSN", raising=False)
    monkeypatch.setattr(run_ingester, "_load_pg_dsn", lambda: "postgresql://secret")
    assert run_ingester.run([str(tmp_path), "dev", "ingestion.batch.mastr_simulator"]) == 0
    assert calls["dsn"] == "postgresql://secret"


def test_meter_replay_gets_kafka_settings_from_secrets(monkeypatch, tmp_path):
    import os
    import sys
    import types

    from src.jobs import run_ingester

    seen = {}
    module = types.ModuleType("ingestion.producers.meter_replay")
    module.main = lambda args: (
        seen.update({k: os.environ.get(k) for k in run_ingester.KAFKA_SECRETS} | {"args": args})
        or 0
    )
    monkeypatch.setitem(sys.modules, "ingestion.producers.meter_replay", module)
    for name in [*run_ingester.KAFKA_SECRETS, "GRIDPULSE_KAFKA_SECURITY"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(run_ingester, "_secret", lambda key: f"secret:{key}")
    argv = [str(tmp_path), "dev", "ingestion.producers.meter_replay", "--days", "1"]
    assert run_ingester.run(argv) == 0
    assert seen["GRIDPULSE_KAFKA_PASSWORD"] == "secret:kafka_password"
    assert seen["GRIDPULSE_KAFKA_BOOTSTRAP"] == "secret:kafka_bootstrap"
    assert seen["args"] == ["--days", "1"]
    assert os.environ["GRIDPULSE_KAFKA_SECURITY"] == "SASL_SSL"
