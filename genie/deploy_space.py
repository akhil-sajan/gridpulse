"""Create or update the GridPulse Genie space from genie/space.py (P6.2).

    python -m genie.deploy_space --catalog gridpulse_dev --title-prefix "[dev] "

Authentication comes from the environment (DATABRICKS_HOST / DATABRICKS_TOKEN in CI, or
DATABRICKS_CONFIG_PROFILE locally). The space is found by its title, so running this
again updates the same space instead of creating a second one.
"""

from __future__ import annotations

import argparse
import json

from databricks.sdk import WorkspaceClient

from genie import space

WAREHOUSE_NAME = "Serverless Starter Warehouse"


def find_space(w: WorkspaceClient, title: str) -> str | None:
    token = None
    while True:
        page = w.genie.list_spaces(page_size=100, page_token=token)
        for s in page.spaces or []:
            if s.title == title:
                return s.space_id
        token = page.next_page_token
        if not token:
            return None


def warehouse_id(w: WorkspaceClient, name: str = WAREHOUSE_NAME) -> str:
    for wh in w.warehouses.list():
        if wh.name == name:
            return wh.id
    raise SystemExit(f"SQL warehouse {name!r} not found")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--title-prefix", default="")
    args = parser.parse_args(argv)

    w = WorkspaceClient()
    title = args.title_prefix + space.TITLE
    payload = json.dumps(space.build(args.catalog))
    wh = warehouse_id(w)
    existing = find_space(w, title)
    if existing:
        w.genie.update_space(
            existing,
            serialized_space=payload,
            title=title,
            description=space.DESCRIPTION,
            warehouse_id=wh,
        )
        print(f"updated Genie space {title!r} ({existing})")
    else:
        created = w.genie.create_space(wh, payload, title=title, description=space.DESCRIPTION)
        print(f"created Genie space {title!r} ({created.space_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
