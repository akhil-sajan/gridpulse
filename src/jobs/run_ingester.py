"""Entry point for ingestion tasks in Databricks jobs.

Usage (as a spark_python_task)::

    run_ingester.py <repo_root> <env> <module> [module args...]

- ``repo_root``: where the bundle synced the repository, so ``ingestion`` imports.
- ``env``: ``dev`` or ``prod``; sets ``GRIDPULSE_ENV`` (catalog and landing volume).
- ``module``: e.g. ``ingestion.batch.smard``; its ``main(args)`` is called.

Inside Databricks the Databricks SDK authenticates automatically, so no token is needed.
The MaStR simulator reads the Neon connection string from the secret scope
``gridpulse`` (key ``pg_dsn``).
The task fails when the ingester reports errors (non-zero exit code).
"""

from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Sequence

ALLOWED_MODULES = {
    "ingestion.batch.smard",
    "ingestion.batch.open_meteo",
    "ingestion.batch.mastr_simulator",
    "ingestion.producers.meter_replay",
}
# Modules that talk to Neon Postgres get the connection string from a secret scope.
NEEDS_PG_DSN = {"ingestion.batch.mastr_simulator"}
PG_SECRET = ("gridpulse", "pg_dsn")
# Modules that send to Kafka get the Redpanda connection from the same secret scope.
NEEDS_KAFKA = {"ingestion.producers.meter_replay"}
KAFKA_SECRETS = {
    "GRIDPULSE_KAFKA_BOOTSTRAP": "kafka_bootstrap",
    "GRIDPULSE_KAFKA_USERNAME": "kafka_username",
    "GRIDPULSE_KAFKA_PASSWORD": "kafka_password",
}


def _secret(key: str) -> str:
    from databricks.sdk.runtime import dbutils

    return dbutils.secrets.get(PG_SECRET[0], key)


def _load_pg_dsn() -> str:
    return _secret(PG_SECRET[1])


def run(argv: Sequence[str]) -> int:
    if len(argv) < 3:
        raise SystemExit("usage: run_ingester.py <repo_root> <env> <module> [args...]")
    repo_root, env, module, *args = argv
    if module not in ALLOWED_MODULES:
        raise SystemExit(f"unknown ingester module {module!r}; allowed: {sorted(ALLOWED_MODULES)}")
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    os.environ["GRIDPULSE_ENV"] = env
    os.environ.setdefault("GRIDPULSE_LANDING_BACKEND", "volume")
    if module in NEEDS_PG_DSN and not os.environ.get("GRIDPULSE_PG_DSN"):
        os.environ["GRIDPULSE_PG_DSN"] = _load_pg_dsn()
    transport = os.environ.get("GRIDPULSE_STREAM_TRANSPORT", "kafka")
    if "--transport" in args[:-1]:
        transport = args[args.index("--transport") + 1]
    if module in NEEDS_KAFKA and transport == "kafka":
        os.environ.setdefault("GRIDPULSE_KAFKA_SECURITY", "SASL_SSL")
        for env_name, key in KAFKA_SECRETS.items():
            if not os.environ.get(env_name):
                os.environ[env_name] = _secret(key)
    return int(importlib.import_module(module).main(list(args)) or 0)


if __name__ == "__main__":
    code = run(sys.argv[1:])
    if code:
        raise SystemExit(code)
