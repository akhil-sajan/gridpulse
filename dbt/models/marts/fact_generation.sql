{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['time_key', 'production_type_key'],
    liquid_clustered_by=['time_key'],
    on_schema_change='fail'
  )
}}
-- Grain: one row per 15-minute interval and production type. Energy in MWh.
-- Incremental: only rows silver changed since the last run (new data or a newer version
-- of an open week) are merged in.
SELECT
    cast(date_format(g.ts_utc, 'yyyyMMddHHmm') AS BIGINT) AS time_key,
    {{ dbt_utils.generate_surrogate_key(['g.series_name']) }} AS production_type_key,
    g.mwh,
    g._source_loaded_at
FROM {{ ref('stg_smard_generation') }} AS g
{% if is_incremental() %}
    WHERE g._source_loaded_at > (SELECT max(t._source_loaded_at) FROM {{ this }} AS t)
{% endif %}
