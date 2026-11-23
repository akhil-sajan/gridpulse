-- Observed hourly weather per state location. The Germany-wide value is built in
-- int_weather_national (capacity-weighted, P3.5).
SELECT
    location_code AS region_code,
    ts_utc,
    shortwave_radiation,
    cloud_cover,
    wind_speed_100m,
    temperature_2m,
    1 AS n_locations,
    _file_modified_at AS _source_loaded_at
FROM {{ source('silver', 'openmeteo_weather') }}
