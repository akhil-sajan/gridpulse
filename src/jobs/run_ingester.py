"""Entry point for ingestion tasks in Databricks jobs.

Usage (as a spark_python_task)::

    run_ingester.py <repo_root> <env> <module> [module args...]

- ``repo_root``: where the bundle synced the repository, so ``ingestion`` imports.
- ``env``: ``dev`` or ``prod``; sets ``GRIDPULSE_ENV`` (catalog and landing volume).
- ``module``: e.g. ``ingestion.batch.smard``; its ``main(args)`` is called.

Inside Databricks the Databricks SDK authenticates automatically, so no token is needed.
The task fails when the ingester reports errors (non-zero exit code).
"""

from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Sequence

ALLOWED_MODULES = {"ingestion.batch.smard", "ingestion.batch.open_meteo"}


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
    return int(importlib.import_module(module).main(list(args)) or 0)


if __name__ == "__main__":
    code = run(sys.argv[1:])
    if code:
        raise SystemExit(code)
