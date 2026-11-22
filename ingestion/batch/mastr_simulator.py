"""Daily change simulator for the power-plant register in Postgres (P3.2).

The real register changes every day, but its export can't be downloaded automatically.
To give the change-data-capture pipeline (P3.3, P3.4) fresh changes every day, this
script plays "the users of the operational database" and makes a handful of realistic
changes, each in the way the real register records them:

* **new units**        a new plant is registered (status "In Planung" or "In Betrieb"),
                       copying location and energy source from a real unit of that type
* **commissioned**     a planned unit goes into operation (commissioning date = today)
* **capacity changed** an operating unit is upgraded or repowered (+5 % to +50 %)
* **temporarily stopped / restarted**  "In Betrieb" <-> "Vorübergehend stillgelegt"
* **decommissioned**   an operating unit closes for good (decommissioning date = today)

Every changed row gets ``updated_at = now()`` (the CDC watermark) and
``change_source = 'simulator'``. New units get IDs starting with ``SIM`` so they can never
be mistaken for real register entries. Nothing is hard-deleted: the register marks closures
with a status, and a watermark-based extract could not see deletes anyway.

All changes of one run happen in one transaction and are logged in
``public.simulator_runs``.

Run from the repo root (``GRIDPULSE_PG_DSN`` must be set)::

    python -m ingestion.batch.mastr_simulator             # one day's changes
    python -m ingestion.batch.mastr_simulator --seed 42   # reproducible run
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal

from ingestion.batch.mastr_load import DDL_PATH, DSN_ENV, connect, sql_statements
from ingestion.common.logs import get_logger

log = get_logger("mastr_simulator")

IN_OPERATION = "In Betrieb"
PLANNED = "In Planung"
TEMP_STOPPED = "Vorübergehend stillgelegt"
CLOSED = "Endgültig stillgelegt"

# Share of new registrations per unit type, roughly like real daily registrations
# among units >= 100 kW (solar and batteries dominate).
NEW_UNIT_WEIGHTS: dict[str, int] = {
    "solar": 55,
    "wind": 15,
    "storage": 15,
    "biomass": 5,
    "combustion": 5,
    "hydro": 5,
}
ID_PREFIX = {
    "solar": "SEE",
    "wind": "SEW",
    "storage": "SSE",
    "biomass": "SBE",
    "combustion": "SVE",
    "hydro": "SWE",
}


@dataclass
class Plan:
    """How many changes of each kind one run makes."""

    new_units: int = 15
    commissioned: int = 10
    capacity_changed: int = 8
    temporarily_stopped: int = 3
    restarted: int = 3
    decommissioned: int = 5


@dataclass
class RunResult:
    new_units: int = 0
    commissioned: int = 0
    capacity_changed: int = 0
    temporarily_stopped: int = 0
    restarted: int = 0
    decommissioned: int = 0

    @property
    def total(self) -> int:
        return sum(asdict(self).values())


def new_unit_id(unit_type: str, run_at: datetime, index: int) -> str:
    """``SIM`` + the real prefix of that type + run time + counter, e.g. SIMSEE202611230617001."""
    return f"SIM{ID_PREFIX[unit_type]}{run_at:%Y%m%d%H%M}{index:03d}"


def capacity_factor(rng: random.Random) -> Decimal:
    """Upgrade factor between 1.05 and 1.50 (repowering usually adds capacity)."""
    return Decimal(str(round(rng.uniform(1.05, 1.50), 3)))


def _pick(conn, status: str, n: int, seed: float) -> list[str]:
    """Pick n random unit IDs with the given status (reproducible for a given seed)."""
    if n <= 0:
        return []
    conn.run("SELECT setseed(:seed)", seed=seed)
    rows = conn.run(
        "SELECT unit_id FROM public.power_units WHERE status = :status ORDER BY random() LIMIT :n",
        status=status,
        n=n,
    )
    return [r[0] for r in rows]


def _set_status(conn, ids: list[str], status: str, extra: str = "") -> int:
    if not ids:
        return 0
    rows = conn.run(
        f"UPDATE public.power_units SET status = :status{extra}, updated_at = now(), "
        "mastr_updated_at = now(), change_source = 'simulator' "
        "WHERE unit_id = ANY(:ids) RETURNING unit_id",
        status=status,
        ids=ids,
    )
    return len(rows)


def _insert_new_units(conn, n: int, rng: random.Random, run_at: datetime) -> int:
    inserted = 0
    types = list(NEW_UNIT_WEIGHTS)
    weights = list(NEW_UNIT_WEIGHTS.values())
    for i in range(1, n + 1):
        unit_type = rng.choices(types, weights)[0]
        # Copy a real unit of the same type: realistic location, energy source and size.
        conn.run("SELECT setseed(:seed)", seed=rng.uniform(-1, 1))
        template = conn.run(
            "SELECT energy_source, federal_state, postcode, net_capacity_kw "
            "FROM public.power_units WHERE unit_type = :t AND change_source = 'mastr_export' "
            "ORDER BY random() LIMIT 1",
            t=unit_type,
        )
        if not template:
            continue
        energy_source, state, postcode, kw = template[0]
        status = IN_OPERATION if rng.random() < 0.3 else PLANNED
        conn.run(
            "INSERT INTO public.power_units (unit_id, unit_type, energy_source, federal_state, "
            "postcode, net_capacity_kw, commissioning_date, status, mastr_updated_at, "
            "updated_at, change_source) VALUES (:id, :t, :es, :st, :pc, :kw, :cd, :status, "
            "now(), now(), 'simulator')",
            id=new_unit_id(unit_type, run_at, i),
            t=unit_type,
            es=energy_source,
            st=state,
            pc=postcode,
            kw=kw,
            cd=run_at.date() if status == IN_OPERATION else None,
            status=status,
        )
        inserted += 1
    return inserted


def simulate(conn, plan: Plan, seed: int | None = None, now: datetime | None = None) -> RunResult:
    """Apply one day's changes in a single transaction and log the run."""
    run_at = now or datetime.now(UTC)
    rng = random.Random(seed)

    def pg_seed() -> float:  # Postgres setseed() takes a value in [-1, 1]
        return rng.uniform(-1, 1)

    result = RunResult()
    conn.run("START TRANSACTION")
    try:
        # Pick all targets first, so one unit isn't changed twice in a run.
        operating = _pick(
            conn,
            IN_OPERATION,
            plan.capacity_changed + plan.temporarily_stopped + plan.decommissioned,
            pg_seed(),
        )
        upgrade = operating[: plan.capacity_changed]
        stop = operating[plan.capacity_changed : plan.capacity_changed + plan.temporarily_stopped]
        close = operating[plan.capacity_changed + plan.temporarily_stopped :]
        commission = _pick(conn, PLANNED, plan.commissioned, pg_seed())
        restart = _pick(conn, TEMP_STOPPED, plan.restarted, pg_seed())

        for unit_id in upgrade:
            conn.run(
                "UPDATE public.power_units SET net_capacity_kw = round(net_capacity_kw * :f, 3), "
                "updated_at = now(), mastr_updated_at = now(), change_source = 'simulator' "
                "WHERE unit_id = :id",
                f=capacity_factor(rng),
                id=unit_id,
            )
        result.capacity_changed = len(upgrade)
        result.temporarily_stopped = _set_status(conn, stop, TEMP_STOPPED)
        result.restarted = _set_status(conn, restart, IN_OPERATION)
        result.decommissioned = _set_status(
            conn, close, CLOSED, ", decommissioning_date = current_date"
        )
        result.commissioned = _set_status(
            conn, commission, IN_OPERATION, ", commissioning_date = current_date"
        )
        result.new_units = _insert_new_units(conn, plan.new_units, rng, run_at)

        conn.run(
            "INSERT INTO public.simulator_runs (run_at, seed, new_units, commissioned, "
            "capacity_changed, temporarily_stopped, restarted, decommissioned) "
            "VALUES (:run_at, :seed, :a, :b, :c, :d, :e, :f)",
            run_at=run_at,
            seed=seed,
            a=result.new_units,
            b=result.commissioned,
            c=result.capacity_changed,
            d=result.temporarily_stopped,
            e=result.restarted,
            f=result.decommissioned,
        )
        conn.run("COMMIT")
    except Exception:
        conn.run("ROLLBACK")
        raise
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulate one day of register changes.")
    parser.add_argument("--seed", type=int, help="random seed for a reproducible run")
    for name, default in asdict(Plan()).items():
        parser.add_argument(f"--{name.replace('_', '-')}", type=int, default=default)
    args = parser.parse_args(argv)

    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        parser.error(f"set {DSN_ENV} first")
    plan = Plan(**{k: getattr(args, k) for k in asdict(Plan())})

    conn = connect(dsn)
    try:
        for statement in sql_statements(DDL_PATH.read_text(encoding="utf-8")):
            conn.run(statement)
        result = simulate(conn, plan, seed=args.seed)
    finally:
        conn.close()

    log.info("simulation finished", extra={**asdict(result), "total": result.total})
    print(f"\nChanges made ({result.total} rows):")
    for name, count in asdict(result).items():
        print(f"  {name:<20}{count:>5}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
