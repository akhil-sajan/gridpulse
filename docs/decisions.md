# Design decisions

A running record of choices made while building GridPulse, and why.

## P0.1 – Databricks workspace (2026-11-21)

- **Platform:** Databricks Free Edition, verified with LinkedIn to enable outbound internet access.
- **Source reachability:** Energy-Charts, Open-Meteo (forecast and archive), SMARD and MaStR are all reachable from the workspace.
- **Unity Catalog:** separate catalogs `gridpulse_dev` and `gridpulse_prod`, each with schemas `bronze`, `silver`, `gold`, `ml`, `ops`. Free Edition allows catalog creation, so no prefixed-schema fallback is needed.
- **Secrets:** Databricks secret scope `gridpulse`; credentials never live in code.
- **LLM for the ops agent:** the workspace offers foundation models (for example `databricks-gpt-oss-120b`, `databricks-meta-llama-3-3-70b-instruct`), so no external LLM API is needed. Final model choice in P6.7.
- **Ingestion location:** ingestion still runs on GitHub Actions, outside Databricks, even though the sources are reachable. This keeps source systems separate from the lakehouse and saves compute quota.

## P0.2 – AWS (2026-11-21)

- **Region:** `eu-central-1` (Frankfurt).
- **Access:** root account protected with MFA; a dedicated IAM user `gridpulse-terraform` for infrastructure as code, used through the AWS CLI profile `gridpulse`. Its permissions are temporarily broad and will be narrowed to least privilege in P5.6.
- **Cost control:** monthly cost budget with email alerts, plus Free Tier usage alerts.

## P0.3 – Repository (2026-11-21)

- **Layout:** one repository for ingestion, pipelines, dbt, infrastructure and docs, deployed with a Databricks Asset Bundle (from P0.4).
- **Quality gates:** `ruff` (lint and format) and `pytest` run on every push and pull request.
- **Python:** 3.11, matching Databricks serverless.

## P0.4 – Databricks Asset Bundle (2026-11-21)

- **Deployment:** all Databricks resources are defined in the repo (`databricks.yml`, `resources/`) and deployed with the Databricks CLI, never created by hand in the UI.
- **Targets:** `dev` (default, development mode: resources prefixed with the user's name, schedules paused) and `prod` (production mode). A `catalog` variable points each target at `gridpulse_dev` or `gridpulse_prod`.
- **No host in the repo:** the workspace URL comes from the local CLI profile, or from CI secrets later, so the bundle works for any workspace.
- **Compute:** jobs define no cluster and run on serverless compute, the only option on Free Edition.
- **Verified:** `bundle validate` and `bundle deploy -t dev` succeeded, and the `bundle_check` job ran on serverless and found all five schemas in `gridpulse_dev`.

## P0.5 – External accounts (2026-11-21)

- **Operational database:** Neon Postgres in AWS eu-central-1 (PostgreSQL 18). Free, reachable from GitHub Actions and Databricks, and supports logical replication for the optional Debezium step. Long extracts and replication will use the direct endpoint, not the pooler.
- **Other accounts:** Kaggle (smart-meter dataset, downloaded in P4.1) and an ENTSO-E API token request (optional cross-border flows, P8.8).
- **Local tooling note:** Windows Smart App Control blocks `psycopg`'s binary DLL on the development laptop, so local connection tests use the pure-Python driver `pg8000`. Pipelines run on GitHub Actions and Databricks, where this does not apply.

## P0.6 – Landing zone (2026-11-21)

- **Decision:** raw files land in our own S3 bucket (`gridpulse-landing-cybtgm`, eu-central-1), exposed to Databricks as Unity Catalog external volumes:
  - `/Volumes/gridpulse_dev/bronze/landing` → `s3://gridpulse-landing-cybtgm/dev/`
  - `/Volumes/gridpulse_prod/bronze/landing` → `s3://gridpulse-landing-cybtgm/prod/`
- **Why not a managed volume:** Free Edition supports storage credentials, so we use a bucket we own. Raw data stays in our AWS account, independent of the workspace, and the setup matches how companies connect Databricks to their cloud storage.
- **Access chain:** IAM role `gridpulse-databricks-landing` (S3 access to this bucket only, trust limited to Databricks' Unity Catalog role with our external ID, plus self-assume) → storage credential `gridpulse_landing_cred` → external location `gridpulse_landing` → external volumes.
- **File events:** the external location has file events enabled with a Databricks-managed queue, so Auto Loader can discover new files from events instead of listing the bucket.
- **Paths don't change:** code always writes to and reads from `/Volumes/<catalog>/bronze/landing/...`; only the storage behind it is S3.
- **Verified:** a file uploaded with the Databricks CLI to the dev volume appeared in `s3://gridpulse-landing-cybtgm/dev/_test/`.
- **Infrastructure as code:** the bucket and IAM role were created with the AWS CLI for now and will be imported into Terraform in P5.6.

## P1.1 – Shared ingestion helpers (2026-11-21)

- **One shared toolkit** in `ingestion/common/` used by every ingester: settings, JSON logging, retrying HTTP client, landing paths and writers. Each ingester only contains source-specific logic.
- **Settings:** `config/settings.yaml` for defaults, `GRIDPULSE_*` environment variables for overrides. No secrets in the file; Databricks credentials are read by the Databricks SDK from the environment or CLI profile.
- **Retries:** network errors, timeouts, HTTP 429 and 5xx are retried with exponential backoff (1s, 2s, 4s… plus up to 10% jitter, capped at 60s, max 5 attempts) and `Retry-After` is honored. Other 4xx errors fail immediately, because retrying a bad request doesn't help.
- **Landing layout:** `/Volumes/<catalog>/bronze/landing/<source>/<entity>/ingest_date=YYYY-MM-DD/<file>`. Source and entity names are validated (lowercase snake_case), file names cannot contain path separators.
- **Writing:** through the Databricks Files API into the Unity Catalog volume, so access stays governed by Unity Catalog; ingesters need Databricks credentials only, not AWS keys. A local writer with the same interface is used in tests.
- **Idempotency:** existing files are skipped unless `overwrite=True`, so re-running an ingester never duplicates raw data.
- **Logs:** one JSON object per line, with extra fields such as `path` and `bytes`, so CI logs can be searched and filtered.
- **Verified:** 40 unit tests (retry, backoff, path building, idempotency, config, logging); the smoke test fetched live Energy-Charts data from a laptop and landed it in the dev volume on S3.

## P1.2 – SMARD backfill (2026-11-21)

- **Unit of download:** SMARD publishes one JSON file per series, resolution and week, so the ingester lands weekly files (not monthly files as first planned).
- **Series:** 12 generation types, grid load, residual load (region DE) and day-ahead prices for the DE-LU bidding zone, hourly for the full history and quarter-hourly where published. Configured in `config/smard.yaml`, not hard-coded.
- **History:** from 2020-01-01 (the week containing it).
- **Raw and unchanged:** files are landed exactly as SMARD returns them; series, region, resolution and week are encoded in the file name.
- **Final vs open weeks:** a week counts as final 2 days after it ends. Final weeks are landed once and skipped afterwards. Open weeks are re-downloaded on every run and landed with a `_partial_<fetch time>` suffix, because SMARD keeps filling and revising them. Silver keeps the latest version per timestamp.
- **No state file:** what has been landed is read from the landing zone itself (file listing), so the landing zone is the single source of truth.
- **Robustness:** downloads run in parallel (default 4 workers); a failed file is recorded and reported without stopping the run; a missing index (HTTP 404) is skipped.
- **Verified:** full backfill landed 5,579 final and 15 partial files with 0 errors; a second run planned only the 15 open weeks. Nuclear has no open week because Germany's last nuclear plants closed in April 2023.

## P1.3 – Open-Meteo weather (2026-11-21)

- **Locations:** one point per German federal state (the state capital), 16 in total, in `config/open_meteo.yaml`. Equal weighting for now; Phase 3 weights states by installed capacity.
- **Variables (hourly, UTC):** shortwave radiation, cloud cover, wind speed at 100 m (around wind-turbine hub height), temperature at 2 m.
- **Archive:** one request per location and calendar month since 2020-01-01. A month is final 7 days after it ends (reanalysis data arrives with a delay); final months are landed once and skipped afterwards, recent months are re-downloaded and landed with a `_partial_<fetch time>` suffix.
- **Forecast:** next ~48 hours (`forecast_days: 3`) for every location, one file per location and fetch time.
- **API limits:** each request's weight is computed (more than 14 days or 10 variables count as several calls), requests are paced to 480 weighted calls per minute (limit 600) and each run is capped at 8,000 (daily limit 10,000). Work that doesn't fit is deferred to the next run.
- **Verified:** full archive landed 1,328 files (1,312 final, 16 partial) with 0 errors using 2,875 weighted calls; a rerun fetched only the 16 current-month files; the forecast landed 16 files.

## P1.4 – Scheduled ingestion (2026-11-21)

- **Where:** GitHub Actions (`.github/workflows/ingest_batch.yml`), outside Databricks, free for public repositories.
- **Schedule (UTC):** Open-Meteo forecast every 3 hours; SMARD and Open-Meteo archive daily at 04:23. Minutes are off the hour because GitHub delays jobs scheduled at :00.
- **Why daily for SMARD (not monthly as first planned):** SMARD adds data to the current week every day; the ingester only re-downloads open weeks, so a daily run is cheap (about 15 files).
- **Why every 3 hours for the forecast (not hourly):** weather models refresh every few hours, and hourly runs would create about 140,000 small files a year for little benefit.
- **Credentials:** `DATABRICKS_HOST` and a 90-day Databricks personal access token in GitHub repository secrets; the Databricks SDK reads them from the environment. Free Edition has no account console for service principals, so a token is the practical option. Rotate before it expires.
- **Writes to:** the dev landing volume (`GRIDPULSE_ENV=dev`). Switch to prod when the prod deployment is in place (P5.5).
- **Isolation:** one job per ingester, so a failing source doesn't block the others; concurrency groups prevent overlapping runs of the same job.
- **Known GitHub behaviour:** schedules in public repositories are disabled after 60 days without repository activity; any push re-enables them.
- **Verified:** a manual run of all three jobs succeeded (SMARD 15 open weeks, forecast 16 files, archive 16 files, 0 errors, each job under a minute).

## P1.5 – Bronze layer (2026-11-21)

- **Engine:** a serverless, triggered Lakeflow declarative pipeline (`gridpulse_bronze`) with Auto Loader streaming tables, defined in `src/bronze/bronze_pipeline.py` and deployed by the bundle.
- **Tables (schema `bronze`):** `smard_generation`, `smard_consumption`, `smard_prices`, `openmeteo_archive`, `openmeteo_forecast`. One row per landed file.
- **Raw payload, explicit schema:** each source's JSON structure is declared up front instead of inferred, so types are stable from the first file; fields the schema doesn't know go to `_rescued_data` (schema evolution mode `rescue`) instead of failing the load.
- **Lineage columns:** `_source_file`, `_file_name`, `_file_modified_at`, `_ingest_date`, `_ingested_at`, plus what the file name says: series, filter, region, resolution and week start for SMARD; location and month for Open-Meteo; whether the file is partial and when it was fetched.
- **Data quality at bronze:** a warn-only expectation that every file name is recognised. Dropping and quarantining rows belongs in silver.
- **Reconciliation:** `src/setup/bronze_check.py` runs after every refresh and fails the job unless each table holds exactly one row per landed file, with no unrecognised names.
- **Contract test:** a unit test checks that the file names the ingesters produce match the regex patterns bronze parses.
- **Schedule:** daily job at 05:15 UTC (pipeline, then reconciliation). Paused in the dev target by development mode; runs on schedule once deployed to prod.
- **File discovery:** directory listing for now; the external location also supports managed file events, an option once file counts grow.
- **Verified:** first run loaded all 7,036 files (SMARD 4,196 + 724 + 724, Open-Meteo 1,360 + 32); reconciliation passed with 0 unrecognised names and 0 rescued rows.

## P2.1 – Silver layer (2026-11-21)

- **Same pipeline as bronze** (`gridpulse_medallion`): Free Edition allows one active pipeline per type, and one pipeline lets Databricks order bronze and silver from their dependencies. Silver tables are published to the `silver` schema by their full names.
- **Unpacking:** each bronze row (a week or month of values in arrays) becomes one row per interval. Values the source hasn't published yet (nulls in open weeks and months) are not rows.
- **Time:** every key is a UTC instant (`ts_utc`, interval start). German local time and date (`ts_local`, `local_date`, Europe/Berlin) are derived attributes. The pipeline's session time zone is set to UTC so nothing depends on cluster defaults. Local days have 92, 96 or 100 quarter-hours; the rules live in `src/utils/timeutils.py` with unit tests and are checked on real data.
- **Latest version wins, incrementally:** AUTO CDC (SCD type 1) keyed by `(series_name, resolution, ts_utc)` for SMARD and `(location_code, ts_utc)` for weather, sequenced by the file's landing time. Partial files for open weeks and months are superseded by later partials and finally by the final file, without reprocessing history.
- **Forecasts keep every run** (`openmeteo_forecast`, key `(location_code, fetched_at, ts_utc)`, with `lead_hours`) so forecast accuracy can be measured later.
- **Quality rules** (`expect_all_or_drop`, a rule evaluating to NULL counts as failed): timestamps present and aligned to the resolution; generation and grid load non-negative (residual load may be negative); prices within the EU day-ahead limits of -500 to 4,000 EUR/MWh; radiation and wind non-negative, cloud cover 0–100 %, temperature between -50 and 50 °C. Rows that fail go to `smard_quarantine` / `openmeteo_quarantine` with the names of the rules they broke.
- **Checks after every refresh** (`src/setup/silver_check.py`): unique keys; no gaps in 2024 between each series' first and last value; correct quarter-hours per German local day; every weather location complete for 2024.
- **Lesson from real data:** the first run failed the completeness check because SMARD's nuclear series ends in late January 2024 (Germany's last nuclear plants closed in April 2023; SMARD kept publishing zeros for a while). Completeness is now measured within each series' active range instead of assuming every series covers the whole year.
- **Observation:** SMARD publishes a quarter-hourly day-ahead price series for the full history, not only since the move to 15-minute day-ahead products in 2025. To be verified in P2.5 before choosing the price series for gold.
- **Verified:** silver checks passed with 0 quarantined rows. Row counts: smard_generation 2,803,104; smard_consumption 483,601; smard_prices 302,160; openmeteo_weather 966,144; openmeteo_forecast 2,304.

## P2.2 + P2.3 – Production categories and national weather (2026-11-21)

- **No unpivot needed:** the plan assumed one column per source, but SMARD already delivers one series per source, so silver is long format from the start (`series_name`, `ts_utc`, `value`).
- **Categories as reference data:** `silver.ref_production_type` maps each of the 12 generation series to `renewable`, `fossil`, `nuclear` or `storage`, with `is_renewable` and `is_weather_dependent` (solar and wind) flags and German and English names. Pumped storage is `storage`, not generation, because it shifts energy in time; "other conventional" (oil, mixed fuels, non-renewable waste) counts as `fossil`. The mapping lives in the pipeline code and a contract test checks it covers exactly the configured series.
- **National weather:** `silver.weather_national_hourly` averages the 16 state locations per UTC hour (equal weights; Phase 3 switches to installed-capacity weights) and carries `n_locations` so incomplete hours are visible. Weather stays hourly; aligning it to 15-minute energy data happens in gold.
- **Verified:** silver checks passed: every generation series has a category, the national weather has one row per hour (60,384 rows) and every 2024 hour is built from all 16 locations.

## P1.4 (revised) – Scheduling moves to Databricks Jobs (2026-11-22)

- **What happened:** the GitHub Actions schedule ran 2 of 5 slots overnight (both forecasts, 40 minutes to 2 hours late) and silently dropped the 04:23 UTC daily run and two forecasts. GitHub's scheduler is best-effort: under load, scheduled runs are delayed or dropped without an error.
- **Decision:** scheduling moves to Databricks Jobs, which keep to their schedule and let ingestion and processing run as one dependency-aware workflow:
  - `gridpulse_refresh`, daily 04:30 UTC: `ingest_smard` and `ingest_open_meteo_archive` in parallel → `refresh_pipeline` (bronze + silver) → `reconcile_bronze` and `check_silver`. The pipeline runs even if one download failed (`run_if: ALL_DONE`), so the other source is still processed; the job is still marked failed and an email goes to the owner.
  - `gridpulse_forecast`, every 3 hours at :20 UTC: Open-Meteo forecast; processed by the next daily refresh.
- **How ingestion runs in Databricks:** `src/jobs/run_ingester.py` is a `spark_python_task` on serverless with a small environment (`requests`, `pyyaml`, `databricks-sdk`). It puts the bundle's synced repository on the Python path and sets `GRIDPULSE_ENV` from the target. Authentication is automatic inside Databricks, so no token is involved.
- **Dev schedules:** development mode pauses schedules, and the CLI refuses to unpause them all at once with a preset. Each ingestion job sets `pause_status: UNPAUSED` itself until prod exists (P5.5).
- **GitHub Actions now:** CI (and CD from P5.5) plus a manual-only fallback workflow for the ingesters. The GitHub token is only needed for that fallback.
- **Airflow considered:** reliable, but it needs an always-on server or a paid managed service, and adds a second orchestration system. Not worth it for this project.
- **Verified:** manual runs of both jobs succeeded inside Databricks; silver grew by the expected amounts (generation +748 rows = 11 active series × 68 quarter-hours, weather +384 = 16 locations × 24 hours, forecasts 80 runs) and all silver checks passed.

## P2.4 – dbt setup (2026-11-22)

- **Split of responsibilities:** Lakeflow builds bronze and silver (streaming, Auto Loader, AUTO CDC); dbt builds gold (business logic as version-controlled, tested SQL with docs and lineage).
- **Versions:** dbt-core 1.12 with dbt-databricks 1.13, installed from `requirements-dbt.txt`, separate from the ingestion requirements so ingestion jobs stay small.
- **Connection:** the Databricks SQL warehouse (Free Edition, 2X-Small). `dbt/profiles.yml` contains no secrets: host, HTTP path and token come from environment variables, so the same file works on a laptop, in GitHub Actions and in a Databricks job. Targets `dev` and `prod` map to the two catalogs.
- **Schemas:** a `generate_schema_name` macro uses the configured schema names as they are: `staging` (views over silver), `gold` (marts), `ml` (feature tables). The catalog already separates environments.
- **Sources and freshness:** the silver tables are dbt sources. Freshness uses the landing time of the newest file behind each table (`_file_modified_at`): warn after 2 days, error after 4; forecasts warn after 12 hours, error after 1 day. Settings live under `config:` as current dbt expects.
- **Packages:** `dbt_utils` and `dbt_expectations` (which brings `dbt_date`).
- **Also added:** a `.sqlfluff` config for SQL linting and a manual GitHub workflow that runs dbt from GitHub's servers.
- **Local tooling note:** the laptop uses Python 3.14, which prints a Pydantic V1 warning; dbt works. A Python 3.12 environment is the fallback if it ever causes problems.
- **Verified:** `dbt debug` passed (connection OK) and `dbt source freshness` passed for all five silver tables.

## P2.5 – Gold star schema in dbt (2026-11-22)

- **Layers:** staging views over silver (schema `staging`, renames and filters only) → gold marts (schema `gold`). Reference data as dbt seeds: `regions` (16 states plus Germany, generated from `config/open_meteo.yaml`) and `holidays_de` (nationwide public holidays 2019–2030, Easter-based dates computed). Contract tests keep both seeds in line with the config and the calendar.
- **dim_time:** one row per 15-minute interval from December 2019 to the end of next year (generated, so it extends itself). `time_key` is the UTC start as `yyyyMMddHHmm`: readable, sortable and unique even in the repeated local hour of the October DST day. German local attributes (date, hour, weekday, quarter-hour of day, public holiday, summer time) come from Europe/Berlin.
- **Other dimensions:** `dim_production_type` and `dim_region` with hashed surrogate keys (`dbt_utils.generate_surrogate_key`); `dim_region` includes Germany as a whole (`de`), so national and state weather share one fact table.
- **Facts:** `fact_generation` (15 min × source, MWh), `fact_consumption` (15 min, grid and residual load), `fact_price` (15 min, EUR/MWh), `fact_weather` (hour × region; keyed by the hour's first 15-minute `time_key`).
- **Incremental where it pays off:** `fact_generation` and `fact_consumption` merge only rows whose silver version changed since the last run (`_source_loaded_at` = landing time of the file behind the row), so revisions of open weeks are picked up however old they are. The smaller facts are rebuilt in full. Large tables use liquid clustering on `time_key`.
- **Price resolution:** `fact_price` uses SMARD's 15-minute price where one exists and otherwise applies the hourly price to all four quarter-hours; `price_resolution` records which. This is correct whichever way SMARD's historical 15-minute series turns out to be built.
- **Tests (40):** unique and not-null keys, relationships from every fact to its dimensions, accepted values (categories, weekdays, price resolution), value ranges (`dbt_expectations`: generation ≥ 0, grid load ≥ 0, price -500 to 4,000, cloud cover 0–100), and a singular test that joining generation, price and weather for one day doesn't multiply rows.
- **Lesson:** the repo ignores `*.csv` to keep raw data out of Git, which also hid the seed files; CI caught it and seeds are now explicitly allowed.
- **Verified:** first `dbt build`: 54 of 54 passed (14 objects built, 40 tests) in about 60 seconds.

## P2.6 – Gold aggregates and docs (2026-11-22)

- **Answer tables in gold:** `agg_renewable_share_daily` (business question 1, one row per German day, with a completeness flag based on 92/96/100 quarter-hours) and `agg_negative_price_hours` (business question 2, one row per UTC hour with at least one negative quarter-hour, with generation mix, load and national weather). Both are full rebuilds; they are small.
- **Definitions:** renewable share = renewable generation / total generation (pumped storage included in the total); renewable share of load = renewable generation / grid load (can exceed 1). From October 2025 an hour counts as negative if any of its quarter-hours is negative.
- **Docs:** model and column descriptions are persisted to Unity Catalog; `dbt docs generate` produces the docs site and lineage graph (`docs/img/dbt_lineage.png`, shown in the README).
- **Validation against official figures:** negative-price hours match the Bundesnetzagentur's published counts exactly for 2022 (69), 2023 (301) and 2024 (457), and the 2024 minimum price (-135.45 EUR/MWh) matches. The 2024 renewable share (58.5 %) is 0.5 points below the official 59.0 % because pumped storage is counted in total generation here.
- **Verified:** `dbt build` 66 of 66 passed (16 models and seeds, 50 tests) in about 68 seconds. Each business question is answered by one short query on an answer table.

## P3.1 Load MaStR into Postgres

- **Source and download:** the full MaStR export (Gesamtdatenexport) from the Bundesnetzagentur, licence DL-DE-BY-2.0. The archive name changes daily and the portal does not allow automated downloads, so the ZIP is downloaded by hand and passed to the loader with `--zip`. This is a one-off seed of the operational database; the daily changes in P3.2 come from the simulator.
- **Parser:** Python's built-in `xml.etree.ElementTree.iterparse` instead of `lxml` (deviation from the plan). The export is UTF-16 XML of several GB; streaming each record straight out of the ZIP and clearing the tree keeps memory flat, and nothing is unpacked to disk. `lxml` would be about twice as fast, but it ships a compiled DLL, and Windows Smart App Control already blocked psycopg's DLL on the development laptop. Measured speed: about 26,000 units per second.
- **Scope:** solar, wind, combustion, biomass, hydro, nuclear and storage units. The plan listed the first three; the other four are small and make the register cover every production type that SMARD reports, including the reactors closed in 2023 (needed for capacity history in P3.5).
- **Size filter:** only units with net capacity of 100 kW or more are loaded. The Neon Free plan allows 1 GB per project, and the register holds millions of rooftop systems below 100 kW. The loader prints, per unit type, the share of total capacity that is kept, so the effect of the filter is measured, not guessed.
- **ELT, not ETL:** catalog codes are translated to the register's own German text (`Hessen`, `In Betrieb`), nothing more. Mapping to GridPulse region and production-type codes happens later in dbt, where it is tested and documented.
- **Two-phase load:** parse to a temporary CSV first, then open the database connection, `COPY` into a temporary staging table and upsert in one transaction. The connection is only open for the short load, so Neon's 5-minute scale-to-zero never interrupts it, and a failed load leaves the table unchanged.
- **Change-aware upsert:** a row is only written, and `updated_at` only bumped, when one of its values differs (`IS DISTINCT FROM`). Re-running the same export changes zero rows. This is what makes `updated_at` a trustworthy watermark for the incremental extract in P3.3. Units that disappear from the export are not deleted; the register marks closures with a status and a decommissioning date instead.
- **Timestamps:** MaStR writes `DatumLetzteAktualisierung` without an offset and with 7 fractional digits. The loader trims to microseconds and the session reads them as Europe/Berlin time.
- **Testing:** unit tests use a small synthetic export in the real format (UTF-16, numbered parts, catalog file). A database test loads it into Postgres, checks that a re-run changes nothing and that one changed unit bumps only its own `updated_at`. CI starts a throwaway Postgres service so this test runs on every push.
- **Result (export of 22 Nov 2026):** 168,265 units loaded (solar 84,738; wind 42,953; biomass 20,196; combustion 14,343; storage 3,255; hydro 2,774; nuclear 6). Capacity kept by the 100 kW filter: wind 100%, nuclear 100%, combustion 99.1%, biomass 97.7%, hydro 96.8%, storage 62.6%, solar 54.5%. Parse time 16 minutes on the laptop. MW totals include planned and closed units; the `status` column separates them.
- **Follow-up for P3.5:** because the filter keeps only about half of solar capacity, P3.5 adds a small aggregate of the units below 100 kW (postcode × unit type × status × commissioning month) so installed-capacity totals are complete without storing millions of rows.
- **Validation (22 Nov 2026 export):** wind units "In Betrieb" total 83.8 GW (31,163 units). Official figures for end of June 2026 are 70.05 GW onshore plus 10.8 GW offshore, about 80.9 GW, before five more months of additions, so the register load is consistent. Solar "In Betrieb" at ≥ 100 kW is 61.1 GW against 125.4 GW installed in total (end of June 2026), matching the measured 54.5 % capacity share of the filter.

## P3.2 Change simulator

- **Why simulate:** the real register changes daily, but its export can't be downloaded automatically and takes 16 minutes to parse. The CDC pipeline needs a source that changes every day on its own, so a simulator acts as the users of the operational database. Real changes can still be brought in at any time by loading a newer export with the P3.1 loader, which only touches rows that really changed.
- **Realistic change types, in the register's own terms:** new registrations (status "In Planung" or "In Betrieb", location, energy source and size copied from a real unit of the same type), commissioning of planned units, capacity upgrades (+5 % to +50 %, like repowering), temporary stops and restarts, and permanent closures with a decommissioning date. About 44 changed rows per run.
- **Simulated data is always identifiable:** `change_source = 'simulator'` on every row the simulator last changed (the loader sets it back to `mastr_export` when a real export overwrites the row), and new units get IDs starting with `SIM`. Every run is logged in `public.simulator_runs`.
- **No hard deletes:** the register records closures with a status, not by deleting rows, and a watermark-based extract (P3.3) cannot see deleted rows at all. This is a known limit of watermark CDC; log-based CDC (Debezium, Phase 8 option) would capture deletes.
- **One transaction per run:** all targets are picked first (so a unit isn't changed twice in one run), then all changes and the run log are committed together. A failed run leaves no trace; tested against Postgres.
- **Runs on GitHub Actions, not Databricks Jobs:** daily at 03:17 UTC, before the 04:30 Databricks refresh. GitHub's scheduler is best-effort (it dropped a run on 22 Nov), but for a simulator a skipped day only means fewer changes. The Neon connection string is a GitHub secret.
- **Runs:** 22 Nov 21:37 UTC (manual), 23 Nov 13:06 UTC (scheduled, started almost 10 hours late by GitHub), 23 Nov 16:35 UTC (manual); 44 changes each. Done criterion met with three runs over two days.
- **Moved to Databricks (P3.3):** after the late GitHub run, and once Databricks was shown to reach Neon on port 5432, the daily simulator became the Databricks job `gridpulse_mastr_simulator` (04:10 UTC). It is defined only in the dev target: it stands in for the source system's users, not the data platform, and a prod copy would double the changes. The GitHub workflow is now manual only.

## P3.3 Incremental extract

- **Connectivity first:** a socket test from a serverless notebook showed Databricks Free Edition can reach Neon on port 5432, and a Spark JDBC test query returned the expected row counts. So the extract runs inside Databricks with Spark's JDBC reader (the PostgreSQL driver is built in), with no landing-zone detour.
- **Lakeflow Connect for Postgres** was considered (the plan's preferred option if available). It needs a gateway on classic compute, which Free Edition does not offer, so the extract is a hand-written job task. Note for interviews: in a full workspace, Lakeflow Connect or Debezium (log-based CDC) would replace it.
- **Credentials:** the Neon connection string lives in the Databricks secret scope `gridpulse` (key `pg_dsn`), set from the laptop with a hidden prompt. Notebooks and logs show secret values as `[REDACTED]`.
- **Window, not just a watermark:** each run reads `updated_at` in `(last window_to, database now − 1 minute]`. Postgres sets `now()` to the start of a transaction, so a long transaction could commit a row whose `updated_at` is already older than the last watermark; the one-minute lag leaves time for such commits. The upper bound comes from the database's clock, not Databricks', so clock differences can't skip rows.
- **Watermark store:** `ops.ingest_watermarks`, one row per run (window, rows extracted, run time). The current watermark is the latest `window_to`. Keeping every run instead of one updatable row gives an audit trail of what each run read.
- **At-least-once:** data is appended to `bronze.mastr_power_units_changes` first and the watermark row second. A crash in between re-reads the same window, so bronze can hold duplicates; the SCD2 step (P3.4) keys on `unit_id` and sequences by `updated_at`, which makes them harmless. Each run has an `_extract_id`, and rows carry their window bounds, so any run can be traced or removed.
- **Serverless details:** `DataFrame.cache()` is not available on serverless, so the run writes straight to bronze and counts rows by `_extract_id` afterwards; Postgres is read once per run.
- **Known limits (documented, accepted):** hard deletes are invisible to watermark CDC, and several changes to one row between two runs arrive as one final state. The register uses status changes instead of deletes, and daily granularity is enough for capacity history.
- **Gotcha:** serverless `spark_python_task` marks a task failed on *any* `SystemExit`, including `sys.exit(0)`. The first two runs extracted correctly but were reported as failed (and auto-retried). Job entry points now only exit on a non-zero code, like `run_ingester.py`.
- **Result (23 Nov 2026):** four extract runs (two of them automatic retries) produced contiguous windows: full copy 168,310 rows, then 0, then exactly the simulator's 44 changes, then 0. Done criterion met: a run with no new changes reads zero rows.

## P3.4 SCD Type 2 dimension

- **AUTO CDC, `STORED AS SCD TYPE 2`,** in the existing declarative pipeline (new library `src/gold/power_plant_pipeline.py`), keyed on `unit_id` and sequenced by `updated_at`. Target `gold.dim_power_plant`: one row per version, `__START_AT`/`__END_AT` mark validity, `__END_AT IS NULL` is the current version.
- **dbt snapshots considered, AUTO CDC chosen:** a dbt snapshot compares the full current table with the last snapshot on every run (a full scan, and only the state at run time is seen). AUTO CDC consumes the change feed incrementally, orders events by `updated_at` (so late or re-delivered rows land in the right version), and handles out-of-order data natively. dbt stays in charge of the facts built on top (P3.5), reading the dimension as a source.
- **What opens a version:** any change to a tracked attribute (status, capacity, location, energy source, dates). `mastr_updated_at`, `change_source` and `updated_at` are kept on each version but are excluded from history tracking, so bookkeeping-only changes (e.g. a reload that only touches the register's own timestamp) don't create empty versions.
- **Re-deliveries removed first:** the extract is at-least-once, so the source view drops duplicate (`unit_id`, `updated_at`) pairs before AUTO CDC. The deduplication state grows by one entry per change (a few dozen a day), which is negligible.
- **Quality:** rows without `unit_id` or `updated_at` are dropped; unknown unit types, unknown statuses and negative capacity are recorded as warnings (visible in the pipeline's data-quality metrics) without dropping the row. A contract test keeps these lists in sync with the loader and simulator.
- **System time vs business time:** `__START_AT`/`__END_AT` say when the register record changed in the operational database (system time). When a plant actually operated is business time: `commissioning_date` and `decommissioning_date`. Because the copy of the register only starts on 22 Nov 2026, capacity history in P3.5 uses business time; the SCD2 versions show how the register itself evolved.
- **Result (23 Nov 2026):** 168,354 versions, 168,325 current units. The 29 closed versions are exactly the units the simulator changed in that day's run (44 changes minus 15 new units); e.g. SEE912632414714 240 kW → 292.08 kW, old version closed at the change's `updated_at`. Done criterion met.

## P3.5 Capacity facts in dbt

- **Small units summed, not stored:** the loader's new `--small-units-only` mode sums units below 100 kW into `public.power_units_small_agg` (unit type × energy source × state × status × commissioning month × decommissioning month), replacing the table as a whole. It leaves `power_units` untouched; re-loading the export there would overwrite the simulated changes and their history. Grouped by state instead of the postcode mentioned in P3.1: capacity facts are per state, and postcodes would have multiplied the groups for no use.
- **Copied as a snapshot:** the extract task copies the summary to `bronze.mastr_small_units_agg` only when Postgres holds a newer load (compares `loaded_at`). It is not a CDC source: it only changes when a new export is loaded.
- **Business time, month granularity:** a unit counts at the end of month M if it was commissioned in or before M and not decommissioned in or before M. Planned units, and units marked closed without a closing date, are left out. Temporarily stopped units still count as installed. The register copy only exists since 22 Nov 2026, so SCD2 validity can't give history back to 2020; commissioning and decommissioning dates can.
- **Mapping to SMARD production types** in the seed `mastr_production_types` (unit type + energy source → series name): Erdgas → natural_gas, Braunkohle → lignite, Steinkohle → hard_coal; oil products, other gases, waste heat, non-biogenic waste, hydrogen → other_conventional. Wind in the exclusive economic zone (`de_awz`, new region row without a weather point) → wind_offshore. Offshore turbines inside the 12-mile zone are registered under their state and count as onshore; the unit's location field was not loaded, so this is a known small undercount of offshore.
- **Storage not mapped:** the register's storage units mix pumped hydro and home/grid batteries, which can't be told apart without the technology field. Left out of `fact_installed_capacity` (documented in the seed's note column) rather than labelled wrongly as pumped storage.
- **Simulated capacity is visible:** `simulated_mw` shows how much of each figure comes from units whose current state was set by the simulator, so real and simulated data never mix unnoticed.
- **Capacity-weighted national weather:** `int_weather_national` replaces the plain average of 16 state capitals. Radiation and cloud cover are weighted by each state's solar capacity in that month, wind speed by onshore wind capacity; temperature stays a plain average. This makes the national weather describe the weather where the plants are, which is what the forecasting features in Phase 6 need.
- **Units without a state:** 65 units ≥ 100 kW have no state in the register (30 storage, 3.6 GW; 30 hydro, 0.9 GW; 5 solar). They are units abroad that feed the German grid (e.g. pumped storage and river plants in Austria and Luxembourg). They get no region and are left out of state-level facts; the dbt warning only fires for state names that are filled in but unknown.
- **Validation (end of June 2026, `month_key = 202606`):** wind onshore 70.5 GW vs 70.05 GW official, offshore 10.3 GW vs 10.8 GW; together 80.8 GW vs 80.85 GW. The offshore gap equals the onshore surplus: farms inside the 12-mile zone are registered under a state (as predicted above). Solar 111.9 GW vs 125.4 GW official (−11 %), of which 54.4 GW comes from the small-unit sums. The gap is most likely net vs gross: the loader uses `Nettonennleistung` (inverter AC limit), while official solar statistics are most likely based on `Bruttoleistung` (module peak), typically about 10 % higher; for wind both are nearly equal, which matches the exact wind result. Confirming it would need `Bruttoleistung` in the loader (an optional follow-up).
