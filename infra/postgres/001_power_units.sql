-- GridPulse operational database (Neon Postgres): power-plant register (P3.1).
--
-- One row per generating unit from the Marktstammdatenregister (MaStR).
-- Catalog values (state, status, energy source) are stored as the register's own
-- German text; mapping to GridPulse region and production-type codes happens in dbt.
--
-- updated_at is the time *this database* last changed the row. It is the watermark
-- that the incremental extract (P3.3) reads, so every writer must set it.
-- mastr_updated_at is the register's own "last updated" timestamp, kept as an attribute.

CREATE TABLE IF NOT EXISTS public.power_units (
    unit_id              text PRIMARY KEY,           -- EinheitMastrNummer, e.g. SEE912345678901
    unit_type            text NOT NULL,              -- solar|wind|combustion|biomass|hydro|nuclear|storage
    energy_source        text,                       -- Energietraeger (catalog text)
    federal_state        text,                       -- Bundesland (catalog text)
    postcode             text,
    net_capacity_kw      numeric(12, 3) NOT NULL,    -- Nettonennleistung
    commissioning_date   date,                       -- Inbetriebnahmedatum
    decommissioning_date date,                       -- DatumEndgueltigeStilllegung
    status               text,                       -- EinheitBetriebsstatus (catalog text)
    mastr_updated_at     timestamptz,                -- DatumLetzteAktualisierung
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS power_units_updated_at_idx ON public.power_units (updated_at);

-- P3.2: who made the row's last change: the real MaStR export or the change simulator.
-- Keeps simulated history clearly separated from real register data.
ALTER TABLE public.power_units
    ADD COLUMN IF NOT EXISTS change_source text NOT NULL DEFAULT 'mastr_export';

-- P3.2: one row per simulator run, so each day's changes can be checked.
CREATE TABLE IF NOT EXISTS public.simulator_runs (
    run_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at            timestamptz NOT NULL DEFAULT now(),
    seed              bigint,
    new_units         integer NOT NULL,
    commissioned      integer NOT NULL,
    capacity_changed  integer NOT NULL,
    temporarily_stopped integer NOT NULL,
    restarted         integer NOT NULL,
    decommissioned    integer NOT NULL
);
