"""Settings for the ingestion scripts: YAML defaults plus environment variables.

Environment variables (all optional):

- ``GRIDPULSE_ENV``: ``dev`` (default) or ``prod``; picks the Unity Catalog catalog.
- ``GRIDPULSE_LANDING_BACKEND``: ``volume`` (default, writes to the Unity Catalog volume)
  or ``local`` (writes to a folder on disk, for tests and offline work).
- ``GRIDPULSE_LOCAL_LANDING_DIR``: root folder for the ``local`` backend (default ``.landing``).
- ``GRIDPULSE_CONFIG``: path to an alternative settings YAML.

Databricks credentials are read by the Databricks SDK itself
(``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` in CI, ``DATABRICKS_CONFIG_PROFILE`` locally).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"
LANDING_BACKENDS = ("volume", "local")


class ConfigError(ValueError):
    """Raised when settings are missing or invalid."""


@dataclass(frozen=True)
class HttpSettings:
    timeout_seconds: float
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    user_agent: str


@dataclass(frozen=True)
class Settings:
    env: str
    catalog: str
    landing_schema: str
    landing_volume: str
    landing_backend: str
    local_landing_dir: Path
    http: HttpSettings

    @property
    def landing_root(self) -> str:
        """Unity Catalog volume path that every landed file sits under."""
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"


def load_settings(
    config_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Load settings from YAML, then apply environment-variable overrides."""
    env_vars = os.environ if environ is None else environ
    path = Path(config_path or env_vars.get("GRIDPULSE_CONFIG") or DEFAULT_CONFIG_PATH)
    if not path.is_file():
        raise ConfigError(f"Settings file not found: {path}")

    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    env = env_vars.get("GRIDPULSE_ENV", "dev").strip().lower()
    environments = raw.get("environments", {})
    if env not in environments:
        raise ConfigError(f"Unknown GRIDPULSE_ENV {env!r}; expected one of {sorted(environments)}")

    backend = env_vars.get("GRIDPULSE_LANDING_BACKEND", "volume").strip().lower()
    if backend not in LANDING_BACKENDS:
        raise ConfigError(
            f"Unknown GRIDPULSE_LANDING_BACKEND {backend!r}; expected one of {LANDING_BACKENDS}"
        )

    try:
        landing = raw["landing"]
        http = raw["http"]
        return Settings(
            env=env,
            catalog=environments[env]["catalog"],
            landing_schema=landing["schema"],
            landing_volume=landing["volume"],
            landing_backend=backend,
            local_landing_dir=Path(env_vars.get("GRIDPULSE_LOCAL_LANDING_DIR", ".landing")),
            http=HttpSettings(
                timeout_seconds=float(http["timeout_seconds"]),
                max_attempts=int(http["max_attempts"]),
                backoff_base_seconds=float(http["backoff_base_seconds"]),
                backoff_max_seconds=float(http["backoff_max_seconds"]),
                user_agent=str(http["user_agent"]),
            ),
        )
    except KeyError as missing:
        raise ConfigError(f"Missing setting in {path}: {missing}") from None


def start_date_arg(value: str) -> date | None:
    """argparse type for ``--start-date``: ISO date, or empty for "use the config".

    Jobs always pass the flag (a bundle variable, empty in dev), so an empty string must
    mean "not set". Used by prod to backfill only a short window (P5.5)."""
    value = value.strip()
    return date.fromisoformat(value) if value else None
