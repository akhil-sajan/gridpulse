{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['time_key'],
    liquid_clustered_by=['time_key'],
    on_schema_change='fail'
  )
}}
-- Grain: one row per 15-minute interval. Grid load and residual load (load minus wind
-- and solar) in MWh per interval. Residual load can be negative.
SELECT
    CAST(date_format(ts_utc, 'yyyyMMddHHmm') AS BIGINT) AS time_key,
    max(CASE WHEN series_name = 'grid_load' THEN mwh END) AS grid_load_mwh,
    max(CASE WHEN series_name = 'residual_load' THEN mwh END) AS residual_load_mwh,
    max(_source_loaded_at) AS _source_loaded_at
FROM {{ ref('stg_smard_consumption') }}
{% if is_incremental() %}
WHERE ts_utc IN (
    SELECT ts_utc FROM {{ ref('stg_smard_consumption') }}
    WHERE _source_loaded_at > (SELECT max(_source_loaded_at) FROM {{ this }})
)
{% endif %}
GROUP BY ts_utc
