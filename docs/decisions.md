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
