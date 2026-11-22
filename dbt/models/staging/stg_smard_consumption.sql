-- Grid load and residual load per 15-minute interval (UTC start).
SELECT
    series_name,
    ts_utc,
    ts_local,
    local_date,
    value AS mwh,
    _file_modified_at AS _source_loaded_at
FROM {{ source('silver', 'smard_consumption') }}
WHERE resolution = 'quarterhour'
