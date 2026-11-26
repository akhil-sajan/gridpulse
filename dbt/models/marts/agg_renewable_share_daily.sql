-- Business question 1: what share of German electricity is renewable, per German day?
-- One row per local (Europe/Berlin) calendar day, with the generation mix, load and prices.
WITH generation AS (
    SELECT
        t.local_date,
        p.category,
        p.is_renewable,
        p.is_weather_dependent,
        g.time_key,
        g.mwh
    FROM {{ ref('fact_generation') }} AS g
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    INNER JOIN {{ ref('dim_production_type') }} AS p USING (production_type_key)
),

daily_generation AS (
    SELECT
        local_date,
        sum(mwh) AS total_generation_mwh,
        sum(CASE WHEN is_renewable THEN mwh ELSE 0 END) AS renewable_mwh,
        sum(CASE WHEN is_weather_dependent THEN mwh ELSE 0 END) AS wind_solar_mwh,
        sum(CASE WHEN category = 'fossil' THEN mwh ELSE 0 END) AS fossil_mwh,
        sum(CASE WHEN category = 'nuclear' THEN mwh ELSE 0 END) AS nuclear_mwh,
        sum(CASE WHEN category = 'storage' THEN mwh ELSE 0 END) AS storage_mwh,
        count(DISTINCT time_key) AS generation_quarter_hours
    FROM generation
    GROUP BY local_date
),

expected AS (
    SELECT
        local_date,
        count(*) AS expected_quarter_hours
    FROM {{ ref('dim_time') }}
    GROUP BY local_date
),

daily_load AS (
    SELECT
        t.local_date,
        sum(c.grid_load_mwh) AS grid_load_mwh,
        sum(c.residual_load_mwh) AS residual_load_mwh
    FROM {{ ref('fact_consumption') }} AS c
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY t.local_date
),

daily_price AS (
    SELECT
        t.local_date,
        avg(p.price_eur_mwh) AS avg_price_eur_mwh,
        min(p.price_eur_mwh) AS min_price_eur_mwh,
        max(p.price_eur_mwh) AS max_price_eur_mwh,
        count_if(p.price_eur_mwh < 0) AS negative_price_quarter_hours
    FROM {{ ref('fact_price') }} AS p
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY t.local_date
)

SELECT
    g.local_date,
    g.total_generation_mwh,
    g.renewable_mwh,
    g.wind_solar_mwh,
    g.fossil_mwh,
    g.nuclear_mwh,
    g.storage_mwh,
    g.renewable_mwh / nullif(g.total_generation_mwh, 0) AS renewable_share,
    g.wind_solar_mwh / nullif(g.total_generation_mwh, 0) AS wind_solar_share,
    l.grid_load_mwh,
    l.residual_load_mwh,
    g.renewable_mwh / nullif(l.grid_load_mwh, 0) AS renewable_share_of_load,
    p.avg_price_eur_mwh,
    p.min_price_eur_mwh,
    p.max_price_eur_mwh,
    p.negative_price_quarter_hours,
    g.generation_quarter_hours,
    e.expected_quarter_hours,
    g.generation_quarter_hours = e.expected_quarter_hours AS is_complete_day
FROM daily_generation AS g
INNER JOIN expected AS e USING (local_date)
LEFT JOIN daily_load AS l USING (local_date)
LEFT JOIN daily_price AS p USING (local_date)
