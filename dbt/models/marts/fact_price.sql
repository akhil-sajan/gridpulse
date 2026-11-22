{{ config(liquid_clustered_by=['time_key']) }}
-- Grain: one row per 15-minute interval. Day-ahead price DE-LU in EUR/MWh.
-- Uses the quarter-hourly price where SMARD publishes one; otherwise the hourly price
-- applies to all four quarter-hours of that hour. price_resolution says which.
WITH quarter_hourly AS (
    SELECT ts_utc, price_eur_mwh, _source_loaded_at
    FROM {{ ref('stg_smard_prices') }}
    WHERE resolution = 'quarterhour'
),

hourly_expanded AS (
    SELECT
        p.ts_utc + make_interval(0, 0, 0, 0, 0, q.offset_minutes, 0) AS ts_utc,
        p.price_eur_mwh,
        p._source_loaded_at
    FROM {{ ref('stg_smard_prices') }} AS p
    CROSS JOIN (SELECT explode(array(0, 15, 30, 45)) AS offset_minutes) AS q
    WHERE p.resolution = 'hour'
),

combined AS (
    SELECT ts_utc, price_eur_mwh, 'quarterhour' AS price_resolution, _source_loaded_at
    FROM quarter_hourly
    UNION ALL
    SELECT h.ts_utc, h.price_eur_mwh, 'hour' AS price_resolution, h._source_loaded_at
    FROM hourly_expanded AS h
    LEFT ANTI JOIN quarter_hourly AS q ON h.ts_utc = q.ts_utc
)

SELECT
    CAST(date_format(ts_utc, 'yyyyMMddHHmm') AS BIGINT) AS time_key,
    price_eur_mwh,
    price_resolution,
    _source_loaded_at
FROM combined
