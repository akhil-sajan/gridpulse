-- Grain: one row per UTC hour with a day-ahead price (every hour, not only negative
-- ones): price, generation, renewable share and grid load. Feeds the dashboard's market
-- and load charts (P6.1); agg_negative_price_hours keeps only the negative hours.
WITH price_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        avg(p.price_eur_mwh) AS avg_price_eur_mwh,
        min(p.price_eur_mwh) AS min_price_eur_mwh,
        count_if(p.price_eur_mwh < 0) AS negative_quarter_hours
    FROM {{ ref('fact_price') }} AS p
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
),

generation_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        sum(g.mwh) AS total_generation_mwh,
        sum(CASE WHEN d.is_renewable THEN g.mwh ELSE 0 END) AS renewable_mwh
    FROM {{ ref('fact_generation') }} AS g
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    INNER JOIN {{ ref('dim_production_type') }} AS d USING (production_type_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
),

load_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        sum(c.grid_load_mwh) AS grid_load_mwh
    FROM {{ ref('fact_consumption') }} AS c
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
)

SELECT
    ht.time_key AS hour_time_key,
    ph.hour_utc,
    ht.ts_local AS hour_local,
    ht.local_date,
    ht.local_hour,
    ht.is_weekend OR ht.is_public_holiday_de AS is_weekend_or_holiday,
    ph.avg_price_eur_mwh,
    ph.min_price_eur_mwh,
    ph.avg_price_eur_mwh < 0 AS is_negative_hour,
    g.total_generation_mwh,
    g.renewable_mwh / nullif(g.total_generation_mwh, 0) AS renewable_share,
    l.grid_load_mwh
FROM price_hourly AS ph
INNER JOIN {{ ref('dim_time') }} AS ht ON ph.hour_utc = ht.ts_utc
LEFT JOIN generation_hourly AS g ON ph.hour_utc = g.hour_utc
LEFT JOIN load_hourly AS l ON ph.hour_utc = l.hour_utc
