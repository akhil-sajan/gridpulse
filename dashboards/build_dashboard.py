"""Writes dashboards/gridpulse.lvdash.json (P6.1).

The AI/BI dashboard is kept as code: datasets (SQL) and widgets are defined here and the
JSON is generated, reviewed in Git and deployed by the bundle (resources/dashboard.yml).
Queries use schema-qualified names (gold.x); the bundle sets the catalog per target
(dataset_catalog), so the same file serves dev and prod.

    python dashboards/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).with_name("gridpulse.lvdash.json")

# Rows of the live stream that are not generation (energy-charts sends them as well).
NOT_GENERATION = (
    "production_type ILIKE '%load%' OR production_type ILIKE '%share%' "
    "OR production_type ILIKE '%trading%' OR production_type ILIKE '%consumption%'"
)

DATASETS = {
    # ----------------------------------------------------------------- page 1: live grid
    "live_mix": (
        "Generation mix, last 24 h (live stream)",
        f"""
SELECT
    from_utc_timestamp(event_ts_utc, 'Europe/Berlin') AS ts_local,
    production_type,
    round(power_mw / 1000, 2) AS power_gw
FROM silver.stream_energy_generation
WHERE event_ts_utc >= (SELECT max(event_ts_utc) FROM silver.stream_energy_generation)
                      - INTERVAL 24 HOURS
  AND NOT ({NOT_GENERATION})
""",
    ),
    "live_kpis": (
        "Latest values",
        f"""
WITH latest AS (
    SELECT max(event_ts_utc) AS ts FROM silver.stream_energy_generation
),
mix AS (
    SELECT g.production_type, g.power_mw
    FROM silver.stream_energy_generation AS g JOIN latest AS l ON g.event_ts_utc = l.ts
    WHERE NOT ({NOT_GENERATION.replace("production_type", "g.production_type")})
),
share AS (
    SELECT
        sum(CASE WHEN production_type ILIKE ANY ('%wind%', '%solar%', '%biomass%',
                     '%geothermal%', '%run-of-river%', '%reservoir%')
                 THEN power_mw ELSE 0 END) / nullif(sum(power_mw), 0) AS renewable_share,
        sum(power_mw) / 1000 AS generation_gw
    FROM mix
),
price AS (
    SELECT p.price_eur_mwh, t.ts_local AS price_ts_local
    FROM gold.fact_price AS p JOIN gold.dim_time AS t USING (time_key)
    WHERE t.ts_utc <= current_timestamp()
    QUALIFY row_number() OVER (ORDER BY t.ts_utc DESC) = 1
),
latency AS (
    SELECT round(percentile_approx(end_to_end_p50_s, 0.5) / 60, 1) AS end_to_end_min
    FROM ops.stream_metrics
    WHERE ingested_minute >= current_timestamp() - INTERVAL 7 DAYS
)
SELECT
    from_utc_timestamp(l.ts, 'Europe/Berlin') AS latest_quarter_hour_local,
    round(s.renewable_share * 100, 1) AS renewable_share_pct,
    round(s.generation_gw, 1) AS generation_gw,
    p.price_eur_mwh,
    lat.end_to_end_min
FROM latest AS l
CROSS JOIN share AS s
CROSS JOIN price AS p
CROSS JOIN latency AS lat
""",
    ),
    "renewable_share_daily": (
        "Renewable share per day, last 90 days",
        """
SELECT local_date, round(renewable_share * 100, 1) AS renewable_share_pct
FROM gold.agg_renewable_share_daily
WHERE is_complete_day AND local_date >= date_sub(current_date(), 90)
""",
    ),
    # ----------------------------------------------------------------- page 2: market
    "negative_hours_monthly": (
        "Negative-price hours per month",
        """
SELECT
    date_trunc('MONTH', local_date) AS month,
    count_if(avg_price_eur_mwh < 0) AS negative_price_hours
FROM gold.agg_negative_price_hours
GROUP BY 1
""",
    ),
    "price_vs_renewables": (
        "Hourly price vs renewable share, last 12 months",
        """
SELECT
    round(renewable_share * 100, 1) AS renewable_share_pct,
    round(avg_price_eur_mwh, 1) AS price_eur_mwh,
    CASE WHEN is_weekend OR is_public_holiday_de THEN 'Weekend / holiday'
         ELSE 'Weekday' END AS day_type
FROM gold.agg_negative_price_hours
WHERE local_date >= add_months(current_date(), -12) AND renewable_share IS NOT NULL
""",
    ),
    "price_profile": (
        "Average price by hour of day, last 12 months",
        """
SELECT
    local_hour,
    CASE WHEN is_weekend OR is_public_holiday_de THEN 'Weekend / holiday'
         ELSE 'Weekday' END AS day_type,
    round(avg(avg_price_eur_mwh), 1) AS avg_price_eur_mwh
FROM gold.agg_negative_price_hours
WHERE local_date >= add_months(current_date(), -12)
GROUP BY 1, 2
""",
    ),
    "market_kpis": (
        "Market figures this year",
        """
SELECT
    count_if(avg_price_eur_mwh < 0) AS negative_hours_this_year,
    round(avg(avg_price_eur_mwh), 1) AS avg_price_this_year,
    round(min(min_price_eur_mwh), 1) AS lowest_price_this_year
FROM gold.agg_negative_price_hours
WHERE year(local_date) = year(current_date())
""",
    ),
    # ----------------------------------------------------------------- page 3: capacity
    "capacity_by_state": (
        "Installed wind + solar capacity per state",
        """
SELECT
    c.month_start,
    r.region_name,
    round(sum(c.installed_mw) / 1000, 2) AS installed_gw
FROM gold.fact_installed_capacity AS c
JOIN gold.dim_region AS r USING (region_key)
JOIN gold.dim_production_type AS p USING (production_type_key)
WHERE p.is_weather_dependent AND NOT r.is_national
GROUP BY 1, 2
""",
    ),
    "capacity_latest": (
        "Installed capacity by state and type, latest month",
        """
SELECT
    r.region_name,
    p.name_en AS production_type,
    round(sum(c.installed_mw) / 1000, 2) AS installed_gw
FROM gold.fact_installed_capacity AS c
JOIN gold.dim_region AS r USING (region_key)
JOIN gold.dim_production_type AS p USING (production_type_key)
WHERE p.is_weather_dependent AND NOT r.is_national
  AND c.month_start = (SELECT max(month_start) FROM gold.fact_installed_capacity)
GROUP BY 1, 2
""",
    ),
    "grid_load_profile": (
        "German grid load by hour of day, last 12 months",
        """
SELECT
    local_hour,
    CASE WHEN is_weekend OR is_public_holiday_de THEN 'Weekend / holiday'
         ELSE 'Weekday' END AS day_type,
    round(avg(grid_load_mwh) / 1000, 1) AS avg_load_gw
FROM gold.agg_negative_price_hours
WHERE local_date >= add_months(current_date(), -12)
GROUP BY 1, 2
""",
    ),
    "household_profile": (
        "Household consumption by hour (London smart meters, replayed stream)",
        """
SELECT
    hour(window_start_utc) AS hour_of_day,
    round(sum(total_kwh) / sum(readings) * 2, 3) AS avg_kw_per_household
FROM gold.fact_meter_consumption_30min
GROUP BY 1
""",
    ),
}


def dataset(name: str) -> dict:
    display, sql = DATASETS[name]
    return {"name": name, "displayName": display, "queryLines": [sql.strip() + "\n"]}


def fields(ds: str, *cols: str) -> list[dict]:
    return [
        {
            "name": "main_query",
            "query": {
                "datasetName": ds,
                "fields": [{"name": c, "expression": f"`{c}`"} for c in cols],
                "disaggregated": True,
            },
        }
    ]


def enc(field: str, kind: str, label: str) -> dict:
    return {"fieldName": field, "scale": {"type": kind}, "displayName": label}


def chart(name, ds, widget_type, title, x, y, color=None, pos=(0, 0, 6, 6)):
    cols = [x[0], y[0]] + ([color[0]] if color else [])
    encodings = {"x": enc(*x), "y": enc(*y)}
    if color:
        encodings["color"] = enc(*color)
    return {
        "widget": {
            "name": name,
            "queries": fields(ds, *cols),
            "spec": {
                "version": 3,
                "widgetType": widget_type,
                "encodings": encodings,
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": dict(zip(("x", "y", "width", "height"), pos, strict=True)),
    }


def counter(name, ds, field, title, pos):
    return {
        "widget": {
            "name": name,
            "queries": fields(ds, field),
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": {"fieldName": field, "displayName": title}},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": dict(zip(("x", "y", "width", "height"), pos, strict=True)),
    }


def text(name, lines, pos):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": dict(zip(("x", "y", "width", "height"), pos, strict=True)),
    }


T, Q, C = "temporal", "quantitative", "categorical"

PAGES = [
    (
        "live",
        "Live grid",
        [
            text(
                "live_intro",
                [
                    "## Germany's power grid, live\n",
                    "Public net generation from energy-charts (Fraunhofer ISE), streamed every "
                    "few minutes; prices from SMARD; updated by the daily pipeline.",
                ],
                (0, 0, 6, 2),
            ),
            counter(
                "kpi_share",
                "live_kpis",
                "renewable_share_pct",
                "Renewable share now (%)",
                (0, 2, 1, 3),
            ),
            counter("kpi_gen", "live_kpis", "generation_gw", "Generation now (GW)", (1, 2, 1, 3)),
            counter(
                "kpi_price", "live_kpis", "price_eur_mwh", "Latest price (EUR/MWh)", (2, 2, 1, 3)
            ),
            counter(
                "kpi_latency",
                "live_kpis",
                "end_to_end_min",
                "Stream latency p50, 7 days (min)",
                (3, 2, 1, 3),
            ),
            counter(
                "kpi_asof",
                "live_kpis",
                "latest_quarter_hour_local",
                "Latest quarter-hour (Berlin)",
                (4, 2, 2, 3),
            ),
            chart(
                "live_mix_area",
                "live_mix",
                "area",
                "Generation mix, last 24 h (GW)",
                ("ts_local", T, "Time (Berlin)"),
                ("power_gw", Q, "GW"),
                ("production_type", C, "Source"),
                (0, 5, 6, 8),
            ),
            chart(
                "share_line",
                "renewable_share_daily",
                "line",
                "Renewable share of generation per day (%)",
                ("local_date", T, "Day"),
                ("renewable_share_pct", Q, "%"),
                None,
                (0, 13, 6, 6),
            ),
        ],
    ),
    (
        "market",
        "Market",
        [
            counter(
                "kpi_neg",
                "market_kpis",
                "negative_hours_this_year",
                "Negative-price hours this year",
                (0, 0, 2, 3),
            ),
            counter(
                "kpi_avg",
                "market_kpis",
                "avg_price_this_year",
                "Average price this year (EUR/MWh)",
                (2, 0, 2, 3),
            ),
            counter(
                "kpi_min",
                "market_kpis",
                "lowest_price_this_year",
                "Lowest hourly price this year (EUR/MWh)",
                (4, 0, 2, 3),
            ),
            chart(
                "neg_bar",
                "negative_hours_monthly",
                "bar",
                "Negative-price hours per month",
                ("month", T, "Month"),
                ("negative_price_hours", Q, "Hours"),
                None,
                (0, 3, 6, 6),
            ),
            chart(
                "scatter",
                "price_vs_renewables",
                "scatter",
                "More renewables, lower prices (hourly, last 12 months)",
                ("renewable_share_pct", Q, "Renewable share (%)"),
                ("price_eur_mwh", Q, "Price (EUR/MWh)"),
                ("day_type", C, "Day"),
                (0, 9, 3, 8),
            ),
            chart(
                "profile",
                "price_profile",
                "line",
                "Average price by hour of day (last 12 months)",
                ("local_hour", Q, "Hour (Berlin)"),
                ("avg_price_eur_mwh", Q, "EUR/MWh"),
                ("day_type", C, "Day"),
                (3, 9, 3, 8),
            ),
        ],
    ),
    (
        "capacity",
        "Capacity and consumption",
        [
            chart(
                "cap_area",
                "capacity_by_state",
                "area",
                "Installed wind + solar per state (GW, end of month)",
                ("month_start", T, "Month"),
                ("installed_gw", Q, "GW"),
                ("region_name", C, "State"),
                (0, 0, 6, 8),
            ),
            chart(
                "cap_bar",
                "capacity_latest",
                "bar",
                "Wind and solar by state, latest month (GW)",
                ("region_name", C, "State"),
                ("installed_gw", Q, "GW"),
                ("production_type", C, "Type"),
                (0, 8, 6, 7),
            ),
            chart(
                "load_profile",
                "grid_load_profile",
                "line",
                "German grid load by hour of day (GW, last 12 months)",
                ("local_hour", Q, "Hour (Berlin)"),
                ("avg_load_gw", Q, "GW"),
                ("day_type", C, "Day"),
                (0, 15, 3, 7),
            ),
            chart(
                "household",
                "household_profile",
                "bar",
                "Household load by hour (kW, London smart meters)",
                ("hour_of_day", Q, "Hour (UTC)"),
                ("avg_kw_per_household", Q, "kW"),
                None,
                (3, 15, 3, 7),
            ),
        ],
    ),
]


def build() -> dict:
    return {
        "datasets": [dataset(n) for n in DATASETS],
        "pages": [
            {"name": name, "displayName": title, "layout": layout, "pageType": "PAGE_TYPE_CANVAS"}
            for name, title, layout in PAGES
        ],
    }


if __name__ == "__main__":
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
