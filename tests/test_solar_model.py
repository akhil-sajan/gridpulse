"""P6.4: split, baseline, model and metrics of the solar forecast (synthetic data)."""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from src.ml import solar  # noqa: E402


def synthetic(days: int = 6 * 365, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    d = pd.date_range("2020-01-01", periods=days, freq="D")
    doy = d.day_of_year.to_numpy()
    season = 0.5 + 0.5 * np.sin(2 * np.pi * (doy - 80) / 365.25)
    clouds = rng.uniform(0, 100, days)
    radiation = 6000 * season * (1 - clouds / 130) + rng.normal(0, 150, days)
    capacity = np.linspace(50_000, 120_000, days)
    cf = np.clip(radiation / 6000 * 0.25, 0.005, 1)
    return pd.DataFrame(
        {
            "local_date": d.date,
            "radiation_wh_m2": radiation,
            "radiation_peak_w_m2": radiation / 8,
            "cloud_cover_pct": clouds,
            "cloud_cover_daylight_pct": clouds,
            "daylight_hours": 8 + 8 * season,
            "temperature_c": 5 + 15 * season,
            "day_of_year_sin": np.sin(2 * np.pi * doy / 365.25),
            "day_of_year_cos": np.cos(2 * np.pi * doy / 365.25),
            "solar_capacity_mw": capacity,
            "solar_capacity_factor": cf,
            "solar_mwh": cf * capacity * 24,
        }
    )


def test_split_is_by_time():
    train, test = solar.split(synthetic())
    assert pd.to_datetime(train["local_date"]).max() <= solar.TRAIN_END
    assert set(pd.to_datetime(test["local_date"]).dt.year) == {2025}


def test_baseline_is_last_years_factor_times_todays_capacity():
    df = synthetic()
    _, test = solar.split(df)
    base = solar.baseline(df, test.head(1))
    last = df[pd.to_datetime(df["local_date"]) == pd.Timestamp("2024-01-01")].iloc[0]
    expected = last["solar_capacity_factor"] * test.iloc[0]["solar_capacity_mw"] * 24
    assert base.iloc[0] == pytest.approx(expected)


def test_model_beats_baseline_on_weather_driven_data():
    df = synthetic()
    train, test = solar.split(df)
    model = solar.train(train)
    m = solar.score(test["solar_mwh"], solar.predict_mwh(model, test))
    b = solar.score(test["solar_mwh"], solar.baseline(df, test))
    assert m.days == b.days == len(test)
    assert m.mape_pct < b.mape_pct


def test_score_skips_missing_and_zero_days():
    s = solar.score(pd.Series([100.0, 0.0, 50.0]), pd.Series([110.0, 5.0, None]))
    assert s.days == 1 and s.mae_mwh == 10 and s.mape_pct == 10


def test_prepare_turns_spark_decimals_into_floats():
    df = synthetic()
    df["solar_capacity_mw"] = [Decimal(str(round(v, 1))) for v in df["solar_capacity_mw"]]
    df["solar_capacity_factor"] = [Decimal(str(round(v, 4))) for v in df["solar_capacity_factor"]]
    prepared = solar.prepare(df)
    assert prepared["solar_capacity_mw"].dtype == "float64"
    _, test = solar.split(prepared)
    assert len(test) == 365
    assert solar.baseline(prepared, test).notna().all()  # the job's crash: float * Decimal
