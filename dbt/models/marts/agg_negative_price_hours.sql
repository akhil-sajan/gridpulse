-- Business question 2: when do day-ahead prices go negative, and what was generating then?
-- One row per UTC hour in which at least one quarter-hour had a negative price.
WITH price_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        avg(p.price_eur_mwh) AS avg_price_eur_mwh,
        min(p.price_eur_mwh) AS min_price_eur_mwh,
        count_if(p.price_eur_mwh < 0) AS negative_quarter_hours
    FROM {{ ref('fact_price') }} AS p
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
    HAVING min(p.price_eur_mwh) < 0
),

generation_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        sum(g.mwh) AS total_generation_mwh,
        sum(CASE WHEN d.is_renewable THEN g.mwh ELSE 0 END) AS renewable_mwh,
        sum(CASE WHEN d.series_name = 'solar' THEN g.mwh ELSE 0 END) AS solar_mwh,
        sum(CASE WHEN d.series_name IN ('wind_onshore', 'wind_offshore') THEN g.mwh ELSE 0 END)
            AS wind_mwh
    FROM {{ ref('fact_generation') }} AS g
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    INNER JOIN {{ ref('dim_production_type') }} AS d USING (production_type_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
),

load_hourly AS (
    SELECT
        date_trunc('HOUR', t.ts_utc) AS hour_utc,
        sum(c.grid_load_mwh) AS grid_load_mwh,
        sum(c.residual_load_mwh) AS residual_load_mwh
    FROM {{ ref('fact_consumption') }} AS c
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    GROUP BY date_trunc('HOUR', t.ts_utc)
),

national_weather AS (
    SELECT
        w.time_key,
        w.shortwave_radiation_w_m2,
        w.wind_speed_100m_kmh,
        w.cloud_cover_pct
    FROM {{ ref('fact_weather') }} AS w
    INNER JOIN {{ ref('dim_region') }} AS r USING (region_key)
    WHERE r.region_code = 'de'
)

SELECT
    ht.time_key AS hour_time_key,
    ph.hour_utc,
    ht.ts_local AS hour_local,
    ht.local_date,
    ht.local_hour,
    ht.local_weekday_name,
    ht.is_weekend,
    ht.is_public_holiday_de,
    ph.avg_price_eur_mwh,
    ph.min_price_eur_mwh,
    ph.negative_quarter_hours,
    g.total_generation_mwh,
    g.renewable_mwh,
    g.renewable_mwh / nullif(g.total_generation_mwh, 0) AS renewable_share,
    g.solar_mwh,
    g.wind_mwh,
    l.grid_load_mwh,
    l.residual_load_mwh,
    w.shortwave_radiation_w_m2,
    w.wind_speed_100m_kmh,
    w.cloud_cover_pct
FROM price_hourly AS ph
INNER JOIN {{ ref('dim_time') }} AS ht ON ht.ts_utc = ph.hour_utc
LEFT JOIN generation_hourly AS g ON g.hour_utc = ph.hour_utc
LEFT JOIN load_hourly AS l ON l.hour_utc = ph.hour_utc
LEFT JOIN national_weather AS w ON w.time_key = ht.time_key
