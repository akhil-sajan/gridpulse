"""Contract tests: dbt seeds must match the ingestion config and the calendar."""

import csv
from datetime import date
from pathlib import Path

from dateutil.easter import easter

from ingestion.batch.open_meteo import load_config

SEEDS = Path(__file__).resolve().parents[1] / "dbt" / "seeds"


def read(name):
    with open(SEEDS / name, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_regions_seed_matches_weather_locations():
    rows = {r["region_code"]: r for r in read("regions.csv")}
    locations = {loc.code: loc for loc in load_config().locations}
    assert set(rows) == set(locations) | {"de", "de_awz"}
    assert rows["de"]["is_national"] == "true"
    assert rows["de_awz"]["is_national"] == "false" and rows["de_awz"]["weather_point"] == ""
    for code, loc in locations.items():
        r = rows[code]
        assert r["region_name"] == loc.state and r["weather_point"] == loc.city
        assert float(r["latitude"]) == loc.lat and float(r["longitude"]) == loc.lon
        assert r["is_national"] == "false"


def test_holidays_seed_has_nine_national_holidays_per_year():
    rows = read("holidays_de.csv")
    years = {}
    for r in rows:
        years.setdefault(date.fromisoformat(r["holiday_date"]).year, []).append(r)
    assert all(len(v) == 9 for v in years.values())
    assert min(years) <= 2020 and max(years) >= 2028


def test_easter_based_holidays_are_correct():
    by_date = {date.fromisoformat(r["holiday_date"]): r["name_en"] for r in read("holidays_de.csv")}
    for year in (2020, 2024, 2026):
        e = easter(year)
        assert by_date[date.fromordinal(e.toordinal() - 2)] == "Good Friday"
        assert by_date[date.fromordinal(e.toordinal() + 1)] == "Easter Monday"
        assert by_date[date.fromordinal(e.toordinal() + 39)] == "Ascension Day"
        assert by_date[date.fromordinal(e.toordinal() + 50)] == "Whit Monday"


def test_mastr_production_types_cover_loader_types_and_known_series():
    import ast

    from ingestion.batch.mastr_load import UNIT_FILES

    pipeline = Path(__file__).resolve().parents[1] / "src" / "silver" / "silver_pipeline.py"
    tree = ast.parse(pipeline.read_text(encoding="utf-8"))
    series = next(
        {row[0] for row in ast.literal_eval(node.value)}
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "PRODUCTION_TYPES" for t in node.targets)
    )
    rows = read("mastr_production_types.csv")
    keys = [(r["unit_type"], r["energy_source"]) for r in rows]
    assert len(keys) == len(set(keys)), "duplicate mapping"
    assert {r["unit_type"] for r in rows} == set(UNIT_FILES)
    for r in rows:
        assert r["series_name"] in series or (r["series_name"] == "" and r["note"]), r
