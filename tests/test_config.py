from pathlib import Path

import pytest

from ingestion.common.config import ConfigError, load_settings


def test_defaults_to_dev_volume_backend():
    settings = load_settings(environ={})
    assert settings.env == "dev"
    assert settings.catalog == "gridpulse_dev"
    assert settings.landing_backend == "volume"
    assert settings.landing_root == "/Volumes/gridpulse_dev/bronze/landing"
    assert settings.http.max_attempts >= 1


def test_env_and_backend_overrides():
    settings = load_settings(
        environ={
            "GRIDPULSE_ENV": "PROD",
            "GRIDPULSE_LANDING_BACKEND": "local",
            "GRIDPULSE_LOCAL_LANDING_DIR": "/tmp/landing",
        }
    )
    assert settings.catalog == "gridpulse_prod"
    assert settings.landing_root == "/Volumes/gridpulse_prod/bronze/landing"
    assert settings.landing_backend == "local"
    assert settings.local_landing_dir == Path("/tmp/landing")


def test_unknown_env_is_rejected():
    with pytest.raises(ConfigError, match="GRIDPULSE_ENV"):
        load_settings(environ={"GRIDPULSE_ENV": "staging"})


def test_unknown_backend_is_rejected():
    with pytest.raises(ConfigError, match="GRIDPULSE_LANDING_BACKEND"):
        load_settings(environ={"GRIDPULSE_LANDING_BACKEND": "ftp"})


def test_missing_setting_is_reported(tmp_path):
    cfg = tmp_path / "settings.yaml"
    cfg.write_text("environments:\n  dev:\n    catalog: x\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="Missing setting"):
        load_settings(config_path=cfg, environ={})


def test_missing_file_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_settings(config_path=tmp_path / "nope.yaml", environ={})
