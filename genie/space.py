"""The GridPulse Genie space as code (P6.2).

``build(catalog)`` returns the space definition (``serialized_space``, version 2):
tables, column hints, joins, one text instruction, example SQL, sample questions and
the 10 benchmark questions with their reference SQL. ``deploy_space.py`` creates or
updates the space through the Genie API; ``evaluate.py`` asks Genie the benchmark
questions and compares its answers with the reference SQL.

Rules the API enforces (checked in tests/test_genie_space.py): IDs are 32 lowercase hex
characters, every ID-keyed list is sorted by ID, tables by identifier and column
configs by column name, at most one text instruction, three-level table names.
"""

from __future__ import annotations

import hashlib

TITLE = "GridPulse: German power market"
DESCRIPTION = (
    "Ask about Germany's electricity generation, renewable share, day-ahead prices and "
    "installed wind and solar capacity per state (SMARD, energy-charts, MaStR; 2020 to "
    "today). Built on the GridPulse gold layer."
)

TABLES = {
    "agg_market_hourly": "Every hour with a day-ahead price: price, generation, renewable "
    "share, grid load, Berlin local hour, weekend/holiday flag.",
    "agg_renewable_share_daily": "One row per German local day: generation by category, "
    "renewable share, grid load and day-ahead price statistics.",
    "dim_production_type": "Generation types (solar, wind_onshore, lignite, ...) with "
    "category and renewable flag.",
    "dim_region": "German federal states (German names), the offshore zone and Germany.",
    "dim_time": "Quarter-hours since 2019-12 with UTC and Berlin local calendar fields.",
    "fact_generation": "Net public generation in MWh per quarter-hour and production type.",
    "fact_installed_capacity": "Installed capacity in MW at the end of each month per "
    "state and production type (MaStR register).",
    "fact_price": "Day-ahead price DE-LU in EUR/MWh per quarter-hour.",
}

COLUMN_CONFIGS = {
    "dim_production_type": [
        ("category", True, "renewable, fossil, nuclear or storage"),
        ("name_en", True, "English name, e.g. 'Wind onshore'"),
        ("series_name", True, "Key: solar, wind_onshore, wind_offshore, lignite, ..."),
    ],
    "dim_region": [
        (
            "region_name",
            True,
            "German state name: Bayern = Bavaria, Niedersachsen = Lower Saxony, "
            "Nordrhein-Westfalen = North Rhine-Westphalia, Hessen = Hesse, "
            "Ausschließliche Wirtschaftszone = offshore zone (EEZ)",
        ),
    ],
}

TEXT_INSTRUCTION = """\
Data: the German power market. Energy is in MWh per quarter-hour (1 TWh = 1,000,000 MWh), \
prices are day-ahead DE-LU in EUR/MWh, capacity is in MW (report GW = MW / 1000).
Time: tables are keyed by UTC; answer calendar questions (year, month, day, hour) with the \
Berlin local fields: dim_time.local_year / local_month / local_date / local_hour, or \
local_date / local_hour in the agg_ tables.
Renewable share: sum(renewable_mwh) / sum(total_generation_mwh) from \
agg_renewable_share_daily, using only rows with is_complete_day = true; show it in %.
A negative-price hour is a row of agg_market_hourly with is_negative_hour = true.
Installed capacity: fact_installed_capacity holds the state at the END of each month; \
'end of 2025' means month_start = 2025-12-01. State questions exclude dim_region rows \
with is_national = true. Region names are German (Bayern = Bavaria).
Generation and prices exist only for Germany as a whole, not per state; per state only \
installed capacity is available. If asked for generation or renewable share of a state, \
say so and offer national figures or the state's installed capacity instead.
"""

JOINS = [
    ("fact_generation", "dim_time", "time_key", "MANY_TO_ONE"),
    ("fact_generation", "dim_production_type", "production_type_key", "MANY_TO_ONE"),
    ("fact_price", "dim_time", "time_key", "MANY_TO_ONE"),
    ("fact_installed_capacity", "dim_region", "region_key", "MANY_TO_ONE"),
    ("fact_installed_capacity", "dim_production_type", "production_type_key", "MANY_TO_ONE"),
]

EXAMPLES = [
    (
        "What was the renewable share per month in 2024?",
        """SELECT date_trunc('MONTH', local_date) AS month,
       round(sum(renewable_mwh) / sum(total_generation_mwh) * 100, 1) AS renewable_share_pct
FROM {c}.gold.agg_renewable_share_daily
WHERE is_complete_day AND year(local_date) = 2024
GROUP BY 1 ORDER BY 1""",
    ),
    (
        "How many negative-price hours were there per year?",
        """SELECT year(local_date) AS year, count_if(is_negative_hour) AS negative_price_hours
FROM {c}.gold.agg_market_hourly
GROUP BY 1 ORDER BY 1""",
    ),
    (
        "Installed onshore wind capacity per state at the end of 2024",
        """SELECT r.region_name, round(sum(c.installed_mw) / 1000, 2) AS installed_gw
FROM {c}.gold.fact_installed_capacity AS c
JOIN {c}.gold.dim_region AS r USING (region_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE p.series_name = 'wind_onshore' AND c.month_start = DATE'2024-12-01' AND NOT r.is_national
GROUP BY 1 ORDER BY 2 DESC""",
    ),
    (
        "Generation by type on 1 May 2024 in GWh",
        """SELECT p.name_en, round(sum(g.mwh) / 1000, 1) AS generation_gwh
FROM {c}.gold.fact_generation AS g
JOIN {c}.gold.dim_time AS t USING (time_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE t.local_date = DATE'2024-05-01'
GROUP BY 1 ORDER BY 2 DESC""",
    ),
    (
        "Average day-ahead price by hour of day in 2024",
        """SELECT local_hour, round(avg(avg_price_eur_mwh), 2) AS avg_price_eur_mwh
FROM {c}.gold.agg_market_hourly
WHERE year(local_date) = 2024
GROUP BY 1 ORDER BY 1""",
    ),
]

SAMPLE_QUESTIONS = [
    "How many negative-price hours were there each year?",
    "Which states have the most solar capacity?",
    "What was the renewable share last month?",
    "Show the average price by hour of day this year",
]

# The test list (P6.2 "done when"): question -> reference SQL. Different from EXAMPLES,
# so Genie can't just copy an answer.
BENCHMARKS = [
    (
        "What was the renewable share of generation in Germany in July 2025?",
        """SELECT round(sum(renewable_mwh) / sum(total_generation_mwh) * 100, 1)
       AS renewable_share_pct
FROM {c}.gold.agg_renewable_share_daily
WHERE is_complete_day AND local_date BETWEEN DATE'2025-07-01' AND DATE'2025-07-31'""",
    ),
    (
        "How many hours had a negative day-ahead price in 2025?",
        """SELECT count_if(is_negative_hour) AS negative_price_hours
FROM {c}.gold.agg_market_hourly WHERE year(local_date) = 2025""",
    ),
    (
        "Which month of 2025 had the most negative-price hours, and how many?",
        """SELECT date_trunc('MONTH', local_date) AS month, count_if(is_negative_hour) AS hours
FROM {c}.gold.agg_market_hourly WHERE year(local_date) = 2025
GROUP BY 1 ORDER BY 2 DESC LIMIT 1""",
    ),
    (
        "What was the average day-ahead price in 2024?",
        """SELECT round(avg(p.price_eur_mwh), 2) AS avg_price_eur_mwh
FROM {c}.gold.fact_price AS p JOIN {c}.gold.dim_time AS t USING (time_key)
WHERE t.local_year = 2024""",
    ),
    (
        "How much solar capacity was installed in Bavaria at the end of 2025?",
        """SELECT round(sum(c.installed_mw) / 1000, 2) AS solar_gw
FROM {c}.gold.fact_installed_capacity AS c
JOIN {c}.gold.dim_region AS r USING (region_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE r.region_name = 'Bayern' AND p.series_name = 'solar' AND c.month_start = DATE'2025-12-01'""",
    ),
    (
        "Which federal state had the most onshore wind capacity at the end of June 2026?",
        """SELECT r.region_name, round(sum(c.installed_mw) / 1000, 2) AS wind_onshore_gw
FROM {c}.gold.fact_installed_capacity AS c
JOIN {c}.gold.dim_region AS r USING (region_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE p.series_name = 'wind_onshore' AND c.month_start = DATE'2026-06-01' AND NOT r.is_national
GROUP BY 1 ORDER BY 2 DESC LIMIT 1""",
    ),
    (
        "On which day in 2025 was the renewable share highest, and what was it?",
        """SELECT local_date, round(renewable_share * 100, 1) AS renewable_share_pct
FROM {c}.gold.agg_renewable_share_daily
WHERE is_complete_day AND year(local_date) = 2025
ORDER BY renewable_share DESC LIMIT 1""",
    ),
    (
        "How much solar power did Germany generate in 2024, in TWh?",
        """SELECT round(sum(g.mwh) / 1000000, 2) AS solar_twh
FROM {c}.gold.fact_generation AS g
JOIN {c}.gold.dim_time AS t USING (time_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE p.series_name = 'solar' AND t.local_year = 2024""",
    ),
    (
        "What was the average price at 1 pm on weekends and public holidays in 2025?",
        """SELECT round(avg(avg_price_eur_mwh), 2) AS avg_price_eur_mwh
FROM {c}.gold.agg_market_hourly
WHERE year(local_date) = 2025 AND local_hour = 13 AND is_weekend_or_holiday""",
    ),
    (
        "By how many GW did installed wind and solar capacity grow from the end of 2020 to "
        "the end of 2025?",
        """SELECT round((sum(CASE WHEN c.month_start = DATE'2025-12-01' THEN c.installed_mw END)
              - sum(CASE WHEN c.month_start = DATE'2020-12-01' THEN c.installed_mw END)) / 1000, 1)
       AS growth_gw
FROM {c}.gold.fact_installed_capacity AS c
JOIN {c}.gold.dim_region AS r USING (region_key)
JOIN {c}.gold.dim_production_type AS p USING (production_type_key)
WHERE p.is_weather_dependent AND NOT r.is_national""",
    ),
]


def gid(*parts: str) -> str:
    """Stable 32-character lowercase hex ID (same input -> same ID on every deploy)."""
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def lines(sql: str) -> list[str]:
    rows = sql.strip().split("\n")
    return [r + "\n" for r in rows[:-1]] + [rows[-1]]


def build(catalog: str) -> dict:
    def ident(table: str) -> str:
        return f"{catalog}.gold.{table}"

    tables = []
    for name in sorted(TABLES):
        t = {"identifier": ident(name), "description": [TABLES[name]]}
        if name in COLUMN_CONFIGS:
            t["column_configs"] = [
                {
                    "column_name": col,
                    "description": [desc],
                    "enable_entity_matching": match,
                    "enable_format_assistance": True,
                }
                for col, match, desc in sorted(COLUMN_CONFIGS[name])
            ]
        tables.append(t)
    tables.sort(key=lambda t: t["identifier"])

    joins = [
        {
            "id": gid("join", left, right),
            "left": {"identifier": ident(left), "alias": left},
            "right": {"identifier": ident(right), "alias": right},
            "sql": [f"`{left}`.`{key}` = `{right}`.`{key}`", f"--rt=FROM_RELATIONSHIP_TYPE_{rt}--"],
        }
        for left, right, key, rt in JOINS
    ]
    examples = [
        {"id": gid("example", q), "question": [q], "sql": lines(sql.format(c=catalog))}
        for q, sql in EXAMPLES
    ]
    benchmarks = [
        {
            "id": gid("benchmark", q),
            "question": [q],
            "answer": [{"format": "SQL", "content": lines(sql.format(c=catalog))}],
        }
        for q, sql in BENCHMARKS
    ]
    samples = [{"id": gid("sample", q), "question": [q]} for q in SAMPLE_QUESTIONS]

    def by_id(items: list[dict]) -> list[dict]:
        return sorted(items, key=lambda x: x["id"])

    return {
        "version": 2,
        "config": {"sample_questions": by_id(samples)},
        "data_sources": {"tables": tables},
        "instructions": {
            "text_instructions": [{"id": gid("text", "main"), "content": lines(TEXT_INSTRUCTION)}],
            "example_question_sqls": by_id(examples),
            "join_specs": by_id(joins),
        },
        "benchmarks": {"questions": by_id(benchmarks)},
    }
