"""Daily solar forecast: data preparation, baseline, model and metrics (P6.4).

Plain pandas / scikit-learn, no Spark or MLflow, so it is unit-tested locally; the job
script ``train_solar.py`` reads the feature table, calls these functions and logs to
MLflow.

The model predicts the **capacity factor** (solar MWh / (capacity x 24 h)) from the
weather and the season, and is turned into MWh with the capacity known before the day.
Solar capacity more than doubled since 2020; predicting the factor lets the model learn
the weather effect instead of the growth in panels.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

FEATURES = [
    "radiation_wh_m2",
    "radiation_peak_w_m2",
    "cloud_cover_pct",
    "cloud_cover_daylight_pct",
    "daylight_hours",
    "temperature_c",
    "day_of_year_sin",
    "day_of_year_cos",
]
TARGET = "solar_capacity_factor"
TRAIN_END = pd.Timestamp("2024-12-31")
TEST_YEAR = 2025

PARAMS = {
    "max_iter": 400,
    "learning_rate": 0.05,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 20,
    "l2_regularization": 0.1,
    "random_state": 42,
}


def split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Time-based split: train up to the end of 2024, test on 2025. No shuffling, so the
    model never sees a day from the period it is judged on."""
    d = pd.to_datetime(df["local_date"])
    train = df[d <= TRAIN_END]
    test = df[d.dt.year == TEST_YEAR]
    return train.reset_index(drop=True), test.reset_index(drop=True)


def baseline(history: pd.DataFrame, days: pd.DataFrame) -> pd.Series:
    """Same day last year, scaled by capacity: last year's capacity factor x this
    day's capacity x 24 h. (29 Feb uses 28 Feb of the year before.)"""
    h = history.assign(local_date=pd.to_datetime(history["local_date"]))
    cf_by_day = h.set_index("local_date")[TARGET]
    last_year = pd.to_datetime(days["local_date"]) - pd.DateOffset(years=1)
    cf = last_year.map(cf_by_day)
    return (cf * days["solar_capacity_mw"].to_numpy() * 24).rename("baseline_mwh")


def train(train_df: pd.DataFrame, params: dict | None = None) -> HistGradientBoostingRegressor:
    model = HistGradientBoostingRegressor(**(params or PARAMS))
    model.fit(train_df[FEATURES], train_df[TARGET])
    return model


def predict_mwh(model, df: pd.DataFrame) -> pd.Series:
    cf = np.clip(model.predict(df[FEATURES]), 0, 1)
    return pd.Series(cf * df["solar_capacity_mw"].to_numpy() * 24, name="model_mwh")


@dataclass(frozen=True)
class Scores:
    mae_mwh: float
    mape_pct: float
    rmse_mwh: float
    days: int

    def as_dict(self, prefix: str) -> dict[str, float]:
        return {
            f"{prefix}_mae_mwh": self.mae_mwh,
            f"{prefix}_mape_pct": self.mape_pct,
            f"{prefix}_rmse_mwh": self.rmse_mwh,
            f"{prefix}_days": self.days,
        }


def score(actual: pd.Series, predicted: pd.Series) -> Scores:
    """MAE and RMSE in MWh, MAPE in %. Days without a prediction are left out."""
    a = pd.Series(actual).reset_index(drop=True)
    p = pd.Series(predicted).reset_index(drop=True)
    ok = p.notna() & a.notna() & (a > 0)
    a, p = a[ok], p[ok]
    err = p - a
    return Scores(
        mae_mwh=round(float(err.abs().mean()), 1),
        mape_pct=round(float((err.abs() / a).mean() * 100), 2),
        rmse_mwh=round(float(np.sqrt((err**2).mean())), 1),
        days=int(ok.sum()),
    )
