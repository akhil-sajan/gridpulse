"""P6.1: the dashboard JSON is generated from build_dashboard.py and stays consistent."""

import json
import re
from pathlib import Path

from dashboards import build_dashboard as b

JSON = Path(b.__file__).with_name("gridpulse.lvdash.json")


def test_json_is_up_to_date():
    assert json.loads(JSON.read_text(encoding="utf-8")) == b.build(), (
        "run: python dashboards/build_dashboard.py"
    )


def test_every_widget_uses_an_existing_dataset_and_its_columns():
    data = b.build()
    sql = {d["name"]: d["queryLines"][0] for d in data["datasets"]}
    for page in data["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                ds = q["query"]["datasetName"]
                assert ds in sql, ds
                for f in q["query"]["fields"]:
                    # each column must be produced by the dataset's SELECT
                    assert re.search(rf"\b{f['name']}\b", sql[ds]), (ds, f["name"])


def test_queries_never_name_a_catalog():
    # the bundle sets the catalog per target (dataset_catalog)
    for d in b.build()["datasets"]:
        assert "gridpulse_dev" not in d["queryLines"][0]
        assert "gridpulse_prod" not in d["queryLines"][0]


def test_layout_fits_the_six_column_grid():
    for page in b.build()["pages"]:
        for item in page["layout"]:
            p = item["position"]
            assert 0 <= p["x"] and p["x"] + p["width"] <= 6, item["widget"]["name"]
