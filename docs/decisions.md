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
