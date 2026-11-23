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
# enter the token without showing it (works in Windows PowerShell 5.1 and PowerShell 7)
$t = Read-Host "Databricks token" -AsSecureString
$env:DATABRICKS_TOKEN = [System.Net.NetworkCredential]::new("", $t).Password

cd dbt
..\.venv\Scripts\dbt deps --profiles-dir .
..\.venv\Scripts\dbt debug --profiles-dir .
..\.venv\Scripts\dbt source freshness --profiles-dir .
```

`DBT_TARGET=prod` switches to the `gridpulse_prod` catalog.

## Run on GitHub

Actions -> **dbt (manual)** -> Run workflow. Uses the `DATABRICKS_HOST`,
`DATABRICKS_HTTP_PATH` and `DATABRICKS_TOKEN` repository secrets.

## If Windows blocks `dbt.exe`

Smart App Control can block the unsigned `dbt.exe` launcher in the venv
("An Application Control policy has blocked this file"). dbt itself is fine; start it
through Python instead, with the same arguments:

```powershell
python -c "from dbt.cli.main import cli; cli()" build
```
