# GridPulse dbt project (gold layer)

Builds the gold star schema from the silver tables that the Lakeflow pipeline maintains.

| Folder | Schema | Materialization |
|---|---|---|
| `models/staging` | `staging` | views over silver |
| `models/marts` | `gold` | tables (facts, dimensions, aggregates) |
| `models/ml` | `ml` | feature tables |

## Run locally

```powershell
# from the repo root, once
.venv\Scripts\python -m pip install -r requirements-dbt.txt

# every new shell
$env:DATABRICKS_HOST = "https://<workspace>.cloud.databricks.com"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<id>"
$env:DATABRICKS_TOKEN = "<personal access token>"

cd dbt
..\.venv\Scripts\dbt deps --profiles-dir .
..\.venv\Scripts\dbt debug --profiles-dir .
..\.venv\Scripts\dbt source freshness --profiles-dir .
```

`DBT_TARGET=prod` switches to the `gridpulse_prod` catalog.

## Run on GitHub

Actions -> **dbt (manual)** -> Run workflow. Uses the `DATABRICKS_HOST`,
`DATABRICKS_HTTP_PATH` and `DATABRICKS_TOKEN` repository secrets.
