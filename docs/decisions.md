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
