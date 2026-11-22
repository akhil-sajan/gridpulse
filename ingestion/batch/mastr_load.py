"""Load the MaStR power-plant register into Postgres (P3.1).

The Marktstammdatenregister (MaStR) is Germany's official register of power plants,
run by the Bundesnetzagentur. Its full export ("Gesamtdatenexport") is a ZIP of
UTF-16 XML files, one family of files per unit type, split into numbered parts:

    EinheitenSolar_1.xml, EinheitenSolar_2.xml, ...  -> <EinheitenSolar><EinheitSolar>...
    EinheitenWind.xml                                -> <EinheitenWind><EinheitWind>...
    Katalogwerte.xml                                 -> numeric codes -> German text

The archive name changes daily and the portal does not allow automated downloads, so
the ZIP is downloaded by hand and passed in with ``--zip``.

The loader works in two phases so the database connection is only open for a short time
(Neon suspends idle computes after 5 minutes):

1. **Parse.** Stream every unit file straight out of the ZIP with ``iterparse`` (memory
   stays flat, nothing is unpacked to disk), keep units with net capacity >= ``--min-kw``,
   translate catalog codes to text and write the rows to a temporary CSV file.
2. **Load.** ``COPY`` the CSV into a temporary staging table, then upsert into
   ``public.power_units``. A row is only touched (and ``updated_at`` only bumped) when one
   of its values really changed, so re-running the same export changes nothing. That
   matters for the incremental extract in P3.3, which reads rows by ``updated_at``.

Run from the repo root::

    python -m ingestion.batch.mastr_load --zip C:\\Downloads\\Gesamtdatenexport_....zip --dry-run
    python -m ingestion.batch.mastr_load --zip C:\\Downloads\\Gesamtdatenexport_....zip

The connection string is read from the ``GRIDPULSE_PG_DSN`` environment variable
(``postgresql://user:password@host/dbname?sslmode=require``). Use Neon's *direct*
endpoint, not the ``-pooler`` one.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import ssl
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from ingestion.common.logs import get_logger

log = get_logger("mastr_load")

DDL_PATH = Path(__file__).resolve().parents[2] / "infra" / "postgres" / "001_power_units.sql"
DSN_ENV = "GRIDPULSE_PG_DSN"

# GridPulse unit type -> MaStR file family. The record element is the singular form
# ("EinheitenSolar" -> "EinheitSolar").
UNIT_FILES: dict[str, str] = {
    "solar": "EinheitenSolar",
    "wind": "EinheitenWind",
    "combustion": "EinheitenVerbrennung",
    "biomass": "EinheitenBiomasse",
    "hydro": "EinheitenWasser",
    "nuclear": "EinheitenKernkraft",
    "storage": "EinheitenStromSpeicher",
}

# Output column -> MaStR field. Catalog fields hold numeric codes that are looked up
# in Katalogwerte.xml.
FIELDS: dict[str, str] = {
    "unit_id": "EinheitMastrNummer",
    "energy_source": "Energietraeger",
    "federal_state": "Bundesland",
    "postcode": "Postleitzahl",
    "net_capacity_kw": "Nettonennleistung",
    "commissioning_date": "Inbetriebnahmedatum",
    "decommissioning_date": "DatumEndgueltigeStilllegung",
    "status": "EinheitBetriebsstatus",
    "mastr_updated_at": "DatumLetzteAktualisierung",
}
CATALOG_COLUMNS = ("energy_source", "federal_state", "status")

COLUMNS = (
    "unit_id",
    "unit_type",
    "energy_source",
    "federal_state",
    "postcode",
    "net_capacity_kw",
    "commissioning_date",
    "decommissioning_date",
    "status",
    "mastr_updated_at",
)

_PART_RE = re.compile(r"^(?P<family>[A-Za-z]+?)(?:_(?P<part>\d+))?\.xml$", re.IGNORECASE)
_FRACTION_RE = re.compile(r"(\.\d{6})\d+")


# --------------------------------------------------------------------------- parsing


def member_names(names: Iterable[str], family: str) -> list[str]:
    """Return the ZIP members of one file family, ordered by part number."""
    found: list[tuple[int, str]] = []
    for name in names:
        match = _PART_RE.match(Path(name).name)
        if match and match["family"].lower() == family.lower():
            found.append((int(match["part"] or 0), name))
    return [name for _, name in sorted(found)]


def iter_records(stream, record_tag: str) -> Iterator[dict[str, str]]:
    """Yield each ``<record_tag>`` element as a {child tag: text} dict.

    Clearing the root after every record keeps memory flat even for the multi-GB solar
    files. The XML declaration tells the parser the file is UTF-16.
    """
    context = ET.iterparse(stream, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == record_tag:
            yield {child.tag: (child.text or "").strip() for child in elem}
            root.clear()


def load_catalog(zf: zipfile.ZipFile) -> dict[str, str]:
    """Map catalog code (as text) -> German label, from Katalogwerte*.xml."""
    catalog: dict[str, str] = {}
    for name in member_names(zf.namelist(), "Katalogwerte"):
        with zf.open(name) as fh:
            for rec in iter_records(fh, "Katalogwert"):
                if rec.get("Id"):
                    catalog[rec["Id"]] = rec.get("Wert", "")
    return catalog


def _timestamp(value: str) -> str:
    # MaStR writes 7 fractional digits (.NET); Postgres keeps 6.
    return _FRACTION_RE.sub(r"\1", value)


def build_row(
    rec: dict[str, str], unit_type: str, catalog: dict[str, str]
) -> dict[str, object] | None:
    """Turn one MaStR record into an output row, or None if it has no ID or capacity."""
    unit_id = rec.get(FIELDS["unit_id"])
    try:
        capacity = Decimal(rec.get(FIELDS["net_capacity_kw"], ""))
    except InvalidOperation:
        return None
    if not unit_id:
        return None

    row: dict[str, object] = {"unit_type": unit_type}
    for column, source in FIELDS.items():
        row[column] = rec.get(source) or None
    for column in CATALOG_COLUMNS:
        code = row[column]
        row[column] = catalog.get(code, code) if code else None
    row["net_capacity_kw"] = capacity
    if row["mastr_updated_at"]:
        row["mastr_updated_at"] = _timestamp(str(row["mastr_updated_at"]))
    return row


@dataclass
class TypeStats:
    seen: int = 0
    kept: int = 0
    invalid: int = 0
    kept_kw: Decimal = Decimal(0)
    skipped_kw: Decimal = Decimal(0)

    @property
    def capacity_share(self) -> float:
        total = self.kept_kw + self.skipped_kw
        return float(self.kept_kw / total) if total else 0.0


@dataclass
class ParseResult:
    stats: dict[str, TypeStats] = field(default_factory=dict)

    @property
    def kept(self) -> int:
        return sum(s.kept for s in self.stats.values())


def parse_export(
    zip_path: Path,
    out: io.TextIOBase,
    unit_types: Iterable[str],
    min_kw: Decimal,
    progress_every: int = 500_000,
) -> ParseResult:
    """Stream the export and write kept units to ``out`` as CSV (no header)."""
    result = ParseResult()
    writer = csv.writer(out)
    with zipfile.ZipFile(zip_path) as zf:
        catalog = load_catalog(zf)
        log.info("catalog loaded", extra={"values": len(catalog)})
        for unit_type in unit_types:
            family = UNIT_FILES[unit_type]
            record_tag = "Einheit" + family[len("Einheiten") :]
            stats = result.stats.setdefault(unit_type, TypeStats())
            members = member_names(zf.namelist(), family)
            if not members:
                log.warning("no files found", extra={"unit_type": unit_type, "family": family})
            for name in members:
                with zf.open(name) as fh:
                    for rec in iter_records(fh, record_tag):
                        stats.seen += 1
                        row = build_row(rec, unit_type, catalog)
                        if row is None:
                            stats.invalid += 1
                        elif row["net_capacity_kw"] >= min_kw:
                            stats.kept += 1
                            stats.kept_kw += row["net_capacity_kw"]
                            writer.writerow(["" if row[c] is None else row[c] for c in COLUMNS])
                        else:
                            stats.skipped_kw += row["net_capacity_kw"]
                        if stats.seen % progress_every == 0:
                            log.info(
                                "parsing",
                                extra={
                                    "unit_type": unit_type,
                                    "seen": stats.seen,
                                    "kept": stats.kept,
                                    "file": name,
                                },
                            )
            log.info(
                "unit type parsed",
                extra={
                    "unit_type": unit_type,
                    "seen": stats.seen,
                    "kept": stats.kept,
                    "invalid": stats.invalid,
                    "capacity_share": round(stats.capacity_share, 4),
                },
            )
    return result


# --------------------------------------------------------------------------- database


def connect(dsn: str):
    """Open a pg8000 connection from a postgresql:// URL (TLS unless sslmode=disable)."""
    import pg8000.native

    url = urlparse(dsn)
    if url.scheme not in ("postgres", "postgresql"):
        raise ValueError("expected a postgresql:// connection string")
    sslmode = parse_qs(url.query).get("sslmode", ["require"])[0]
    return pg8000.native.Connection(
        user=unquote(url.username or ""),
        password=unquote(url.password or ""),
        host=url.hostname or "localhost",
        port=url.port or 5432,
        database=(url.path or "/").lstrip("/") or "postgres",
        ssl_context=None if sslmode == "disable" else ssl.create_default_context(),
        timeout=600,
    )


_COLS = ", ".join(COLUMNS)
_CHANGED = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in COLUMNS if c != "unit_id")
_SET = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "unit_id")

MERGE_SQL = f"""
WITH src AS (
    SELECT DISTINCT ON (unit_id) {_COLS}
    FROM power_units_stage
    ORDER BY unit_id, mastr_updated_at DESC NULLS LAST
),
changed AS (
    SELECT s.* FROM src s
    LEFT JOIN public.power_units t USING (unit_id)
    WHERE t.unit_id IS NULL OR {_CHANGED}
),
upserted AS (
    INSERT INTO public.power_units ({_COLS}, updated_at)
    SELECT {_COLS}, now() FROM changed
    ON CONFLICT (unit_id) DO UPDATE SET {_SET}, updated_at = now()
    RETURNING (xmax = 0) AS inserted
)
SELECT
    count(*) FILTER (WHERE inserted) AS inserted,
    count(*) FILTER (WHERE NOT inserted) AS updated
FROM upserted
"""

SUMMARY_SQL = """
SELECT unit_type, count(*) AS units, round(sum(net_capacity_kw) / 1000, 1) AS mw
FROM public.power_units
GROUP BY unit_type
ORDER BY unit_type
"""


def sql_statements(script: str) -> list[str]:
    """Split a simple SQL script (no semicolons inside strings) into statements."""
    lines = [line.split("--", 1)[0] for line in script.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def load_csv(conn, csv_path: Path) -> tuple[int, int]:
    """COPY the parsed rows into staging and upsert them. Returns (inserted, updated)."""
    for statement in sql_statements(DDL_PATH.read_text(encoding="utf-8")):
        conn.run(statement)
    # MaStR timestamps carry no offset; they are German local time.
    conn.run("SET TIME ZONE 'Europe/Berlin'")
    conn.run("START TRANSACTION")
    try:
        conn.run(
            "CREATE TEMP TABLE power_units_stage "
            "(LIKE public.power_units INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with csv_path.open("rb") as fh:
            conn.run(f"COPY power_units_stage ({_COLS}) FROM STDIN WITH (FORMAT csv)", stream=fh)
        [[inserted, updated]] = conn.run(MERGE_SQL)
        conn.run("COMMIT")
    except Exception:
        conn.run("ROLLBACK")
        raise
    return int(inserted), int(updated)


# --------------------------------------------------------------------------- CLI


def _print_parse_table(result: ParseResult, min_kw: Decimal) -> None:
    print(f"\nUnits with net capacity >= {min_kw} kW")
    print(f"{'unit_type':<11}{'in export':>12}{'kept':>10}{'kept MW':>12}{'capacity kept':>15}")
    for unit_type, s in result.stats.items():
        print(
            f"{unit_type:<11}{s.seen:>12,}{s.kept:>10,}"
            f"{float(s.kept_kw) / 1000:>12,.0f}{s.capacity_share:>14.1%}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load the MaStR export into Postgres.")
    parser.add_argument("--zip", required=True, type=Path, help="Gesamtdatenexport ZIP file")
    parser.add_argument(
        "--min-kw",
        type=Decimal,
        default=Decimal(100),
        help="keep units with at least this net capacity (default 100)",
    )
    parser.add_argument(
        "--types",
        default=",".join(UNIT_FILES),
        help=f"comma-separated unit types (default {','.join(UNIT_FILES)})",
    )
    parser.add_argument("--dry-run", action="store_true", help="parse and count only")
    args = parser.parse_args(argv)

    unit_types = [t.strip() for t in args.types.split(",") if t.strip()]
    unknown = sorted(set(unit_types) - set(UNIT_FILES))
    if unknown:
        parser.error(f"unknown unit types: {', '.join(unknown)}")
    if not args.zip.is_file():
        parser.error(f"ZIP not found: {args.zip}")
    dsn = os.environ.get(DSN_ENV)
    if not args.dry_run and not dsn:
        parser.error(f"set {DSN_ENV} first (or use --dry-run)")

    started = time.monotonic()
    fd, tmp = tempfile.mkstemp(prefix="mastr_", suffix=".csv")
    csv_path = Path(tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            result = parse_export(args.zip, out, unit_types, args.min_kw)
        _print_parse_table(result, args.min_kw)
        log.info(
            "parse finished",
            extra={"rows": result.kept, "seconds": round(time.monotonic() - started)},
        )
        if args.dry_run:
            return 0

        conn = connect(dsn)
        try:
            inserted, updated = load_csv(conn, csv_path)
            summary = conn.run(SUMMARY_SQL)
        finally:
            conn.close()
    finally:
        csv_path.unlink(missing_ok=True)

    log.info(
        "load finished",
        extra={
            "inserted": inserted,
            "updated": updated,
            "seconds": round(time.monotonic() - started),
        },
    )
    print(f"\nInserted {inserted:,}, updated {updated:,}. public.power_units now holds:")
    print(f"{'unit_type':<11}{'units':>10}{'MW':>12}")
    for unit_type, units, mw in summary:
        print(f"{unit_type:<11}{units:>10,}{float(mw):>12,.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
