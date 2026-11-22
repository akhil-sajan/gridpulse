"""Tests for the MaStR loader (P3.1), using a tiny synthetic export in the real format."""

from __future__ import annotations

import csv
import io
import os
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from ingestion.batch import mastr_load as m

CATALOG = {
    "1402": "Hessen",
    "1410": "Bayern",
    "35": "In Betrieb",
    "37": "In Planung",
    "2495": "Solare Strahlungsenergie",
    "2497": "Wind",
    "2410": "Erdgas",
}


def _xml(root: str, record: str, rows: list[dict[str, str]]) -> bytes:
    body = "".join(
        f"<{record}>" + "".join(f"<{k}>{v}</{k}>" for k, v in r.items()) + f"</{record}>"
        for r in rows
    )
    text = f'<?xml version="1.0" encoding="UTF-16"?><{root}>{body}</{root}>'
    return text.encode("utf-16")  # with BOM, like the real export


def _unit(
    uid: str,
    kw: str,
    state: str = "1402",
    status: str = "35",
    source: str = "2495",
    updated: str = "2024-03-01T10:15:30.1234567",
    **extra: str,
) -> dict[str, str]:
    rec = {
        "EinheitMastrNummer": uid,
        "Nettonennleistung": kw,
        "Bundesland": state,
        "EinheitBetriebsstatus": status,
        "Energietraeger": source,
        "Postleitzahl": "36037",
        "Inbetriebnahmedatum": "2015-06-30",
        "DatumLetzteAktualisierung": updated,
    }
    rec.update(extra)
    return rec


@pytest.fixture
def export_zip(tmp_path: Path) -> Path:
    path = tmp_path / "Gesamtdatenexport_20261122_26.2.zip"
    catalog_rows = [{"Id": k, "Wert": v, "KatalogKategorieId": "1"} for k, v in CATALOG.items()]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Katalogwerte.xml", _xml("Katalogwerte", "Katalogwert", catalog_rows))
        # Solar is split into parts; part 10 must come after part 2.
        zf.writestr(
            "EinheitenSolar_10.xml",
            _xml("EinheitenSolar", "EinheitSolar", [_unit("SEE3", "250.5", state="1410")]),
        )
        zf.writestr(
            "EinheitenSolar_1.xml",
            _xml("EinheitenSolar", "EinheitSolar", [_unit("SEE1", "9.8"), _unit("SEE2", "100")]),
        )
        zf.writestr(
            "EinheitenSolar_2.xml",
            _xml(
                "EinheitenSolar", "EinheitSolar", [_unit("SEE4", "not-a-number"), _unit("", "500")]
            ),
        )
        zf.writestr(
            "EinheitenWind.xml",
            _xml(
                "EinheitenWind",
                "EinheitWind",
                [
                    _unit(
                        "SEW1",
                        "4200",
                        source="2497",
                        status="37",
                        DatumEndgueltigeStilllegung="2030-01-01",
                    )
                ],
            ),
        )
        zf.writestr(
            "EinheitenVerbrennung.xml",
            _xml(
                "EinheitenVerbrennung", "EinheitVerbrennung", [_unit("SEV1", "50", source="2410")]
            ),
        )
    return path


def _parse(zip_path: Path, types=("solar", "wind", "combustion"), min_kw="100"):
    out = io.StringIO()
    result = m.parse_export(zip_path, out, types, Decimal(min_kw))
    rows = [dict(zip(m.COLUMNS, r, strict=True)) for r in csv.reader(io.StringIO(out.getvalue()))]
    return result, rows


def test_member_names_orders_parts_numerically():
    names = [
        "EinheitenSolar_10.xml",
        "EinheitenSolar_2.xml",
        "EinheitenSolarSpeicher.xml",
        "EinheitenSolar_1.xml",
        "EinheitenWind.xml",
    ]
    assert m.member_names(names, "EinheitenSolar") == [
        "EinheitenSolar_1.xml",
        "EinheitenSolar_2.xml",
        "EinheitenSolar_10.xml",
    ]
    assert m.member_names(names, "EinheitenWind") == ["EinheitenWind.xml"]


def test_parse_keeps_units_at_or_above_threshold(export_zip):
    result, rows = _parse(export_zip)
    assert [r["unit_id"] for r in rows] == ["SEE2", "SEE3", "SEW1"]
    solar = result.stats["solar"]
    assert (solar.seen, solar.kept, solar.invalid) == (5, 2, 2)
    assert solar.kept_kw == Decimal("350.5")
    assert solar.skipped_kw == Decimal("9.8")
    assert result.stats["combustion"].kept == 0
    assert result.kept == 3


def test_parse_translates_catalog_codes_and_trims_fractions(export_zip):
    _, rows = _parse(export_zip)
    see3 = next(r for r in rows if r["unit_id"] == "SEE3")
    assert see3["federal_state"] == "Bayern"
    assert see3["status"] == "In Betrieb"
    assert see3["energy_source"] == "Solare Strahlungsenergie"
    assert see3["unit_type"] == "solar"
    assert see3["mastr_updated_at"] == "2024-03-01T10:15:30.123456"
    wind = next(r for r in rows if r["unit_id"] == "SEW1")
    assert wind["status"] == "In Planung"
    assert wind["decommissioning_date"] == "2030-01-01"
    assert see3["decommissioning_date"] == ""  # empty -> NULL in COPY


def test_unknown_catalog_code_is_kept_as_code():
    row = m.build_row(_unit("X", "200", state="9999"), "solar", CATALOG)
    assert row["federal_state"] == "9999"


def test_capacity_share():
    s = m.TypeStats(kept_kw=Decimal(75), skipped_kw=Decimal(25))
    assert s.capacity_share == 0.75
    assert m.TypeStats().capacity_share == 0.0


def test_sql_statements_splits_ddl():
    statements = m.sql_statements(m.DDL_PATH.read_text(encoding="utf-8"))
    assert len(statements) == 4
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS public.power_units")
    assert all("--" not in s for s in statements)


def test_main_dry_run(export_zip, capsys):
    assert m.main(["--zip", str(export_zip), "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "solar" in out and "kept MW" in out


def test_main_requires_dsn(export_zip, monkeypatch):
    monkeypatch.delenv(m.DSN_ENV, raising=False)
    with pytest.raises(SystemExit):
        m.main(["--zip", str(export_zip)])


def test_main_rejects_unknown_type(export_zip):
    with pytest.raises(SystemExit):
        m.main(["--zip", str(export_zip), "--types", "solar,fusion", "--dry-run"])


def test_connect_rejects_non_postgres_url():
    with pytest.raises(ValueError):
        m.connect("mysql://u:p@h/db")


# --------------------------------------------------------------- integration (optional)

TEST_DSN = os.environ.get("GRIDPULSE_TEST_PG_DSN")


@pytest.mark.skipif(not TEST_DSN, reason="set GRIDPULSE_TEST_PG_DSN to run against Postgres")
def test_load_is_idempotent_and_detects_changes(export_zip, tmp_path):
    conn = m.connect(TEST_DSN)
    try:
        conn.run("DROP TABLE IF EXISTS public.power_units, public.simulator_runs")
        csv_path = tmp_path / "rows.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as out:
            m.parse_export(export_zip, out, ["solar", "wind"], Decimal(100))

        assert m.load_csv(conn, csv_path) == (3, 0)
        first = dict(conn.run("SELECT unit_id, updated_at FROM public.power_units"))
        assert m.load_csv(conn, csv_path) == (0, 0)  # same export: nothing touched

        # One unit changes capacity in the next export.
        text = csv_path.read_text(encoding="utf-8").replace(
            "SEE2,solar,Solare Strahlungsenergie,Hessen,36037,100,",
            "SEE2,solar,Solare Strahlungsenergie,Hessen,36037,120,",
        )
        csv_path.write_text(text, encoding="utf-8")
        assert m.load_csv(conn, csv_path) == (0, 1)
        after = dict(conn.run("SELECT unit_id, updated_at FROM public.power_units"))
        assert after["SEE2"] > first["SEE2"]
        assert after["SEE3"] == first["SEE3"]
        [[kw]] = conn.run("SELECT net_capacity_kw FROM public.power_units WHERE unit_id='SEE2'")
        assert kw == Decimal("120.000")
    finally:
        conn.run("DROP TABLE IF EXISTS public.power_units, public.simulator_runs")
        conn.close()
