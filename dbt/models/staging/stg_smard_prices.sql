-- Day-ahead prices DE-LU, hourly and quarter-hourly series (UTC start).
SELECT
    resolution,
    ts_utc,
    value AS price_eur_mwh,
    _file_modified_at AS _source_loaded_at
FROM {{ source('silver', 'smard_prices') }}
WHERE series_name = 'day_ahead'
