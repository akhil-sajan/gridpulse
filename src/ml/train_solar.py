"""Train, track and register the daily solar forecast (P6.4). Databricks job script.

    train_solar.py <repo_root> <catalog>

1. reads ``<catalog>.ml.features_solar_daily``,
2. scores the baseline (same day last year, scaled by capacity) and a gradient-boosting
   model on 2025 after training on 2020-2024,
3. logs parameters, MAE / MAPE / RMSE and a forecast plot to the MLflow experiment
   ``/Users/<me>/gridpulse/solar_forecast_<catalog>``,
4. registers the model in Unity Catalog as ``<catalog>.ml.solar_forecast`` and gives the
   new version the alias ``champion`` only if it beats the baseline and the current
   champion on MAPE.
"""

from __future__ import annotations

import sys

repo_root, catalog = sys.argv[1], sys.argv[2]
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from mlflow.models import infer_signature  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from src.ml import solar  # noqa: E402

spark = SparkSession.builder.getOrCreate()
MODEL_NAME = f"{catalog}.ml.solar_forecast"


def current_champion_mape(client: MlflowClient) -> float | None:
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, "champion")
    except Exception:  # noqa: BLE001 - no model or no alias yet
        return None
    run = client.get_run(version.run_id)
    return run.data.metrics.get("model_mape_pct")


def plot(test, model_mwh, baseline_mwh, path: str) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(test["local_date"], test["solar_mwh"] / 1000, label="actual", color="#222", lw=1.2)
    ax.plot(test["local_date"], model_mwh / 1000, label="model", color="#e07b00", lw=1)
    ax.plot(
        test["local_date"],
        baseline_mwh / 1000,
        label="baseline (last year)",
        color="#5b8db8",
        lw=0.8,
        alpha=0.7,
    )
    ax.set_title(f"Daily solar generation in Germany, {solar.TEST_YEAR}: forecast vs actual")
    ax.set_ylabel("GWh per day")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main() -> int:
    df = solar.prepare(spark.read.table(f"{catalog}.ml.features_solar_daily").toPandas())
    train_df, test_df = solar.split(df)
    print(
        f"rows: {len(df)}  train: {len(train_df)} (to {solar.TRAIN_END.date()})  "
        f"test: {len(test_df)} ({solar.TEST_YEAR})"
    )

    me = spark.sql("SELECT current_user()").first()[0]
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(f"/Users/{me}/gridpulse_solar_forecast_{catalog}")
    client = MlflowClient()

    with mlflow.start_run(run_name="hist_gradient_boosting") as run:
        model = solar.train(train_df)
        model_mwh = solar.predict_mwh(model, test_df)
        base_mwh = solar.baseline(df, test_df)
        m = solar.score(test_df["solar_mwh"], model_mwh)
        b = solar.score(test_df["solar_mwh"], base_mwh)

        mlflow.log_params(
            {
                **solar.PARAMS,
                "features": ",".join(solar.FEATURES),
                "target": solar.TARGET,
                "train_end": str(solar.TRAIN_END.date()),
                "test_year": solar.TEST_YEAR,
                "train_rows": len(train_df),
            }
        )
        mlflow.log_metrics({**m.as_dict("model"), **b.as_dict("baseline")})
        plot(test_df, model_mwh, base_mwh, "/tmp/forecast_vs_actual.png")
        mlflow.log_artifact("/tmp/forecast_vs_actual.png")

        signature = infer_signature(test_df[solar.FEATURES], model.predict(test_df[solar.FEATURES]))
        info = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=signature,
            input_example=test_df[solar.FEATURES].head(3),
            registered_model_name=MODEL_NAME,
        )
        version = info.registered_model_version

    print(f"\nTest {solar.TEST_YEAR} ({m.days} days)")
    print(f"  {'':10} {'MAE MWh':>10} {'MAPE %':>8} {'RMSE MWh':>10}")
    print(f"  {'baseline':10} {b.mae_mwh:>10} {b.mape_pct:>8} {b.rmse_mwh:>10}")
    print(f"  {'model':10} {m.mae_mwh:>10} {m.mape_pct:>8} {m.rmse_mwh:>10}")

    champion = current_champion_mape(client)
    beats_baseline = m.mape_pct < b.mape_pct
    beats_champion = champion is None or m.mape_pct <= champion
    if beats_baseline and beats_champion:
        client.set_registered_model_alias(MODEL_NAME, "champion", version)
        print(f"\n{MODEL_NAME} version {version} is now 'champion' (MAPE {m.mape_pct} %)")
    else:
        print(
            f"\nversion {version} registered, champion unchanged "
            f"(beats baseline: {beats_baseline}, current champion MAPE: {champion})"
        )
    print(f"MLflow run: {run.info.run_id}")
    if not beats_baseline:
        raise SystemExit("model does not beat the baseline")
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
