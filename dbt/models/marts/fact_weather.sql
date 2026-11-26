{{ config(liquid_clustered_by=['time_key']) }}
-- Grain: one row per hour (UTC start, keyed by the hour's first 15-minute time_key)
-- and region: 16 state points plus the Germany-wide value ('de'), which is weighted by
-- installed capacity (int_weather_national, P3.5).
WITH weather AS (
    SELECT * FROM {{ ref('stg_openmeteo_weather') }}
    UNION ALL
    SELECT * FROM {{ ref('int_weather_national') }}
)

SELECT
    cast(date_format(w.ts_utc, 'yyyyMMddHHmm') AS BIGINT) AS time_key,
    {{ dbt_utils.generate_surrogate_key(['w.region_code']) }} AS region_key,
    w.shortwave_radiation AS shortwave_radiation_w_m2,
    w.cloud_cover AS cloud_cover_pct,
    w.wind_speed_100m AS wind_speed_100m_kmh,
    w.temperature_2m AS temperature_2m_c,
    w.n_locations,
    w._source_loaded_at
FROM weather AS w
