-- Observed hourly weather per state location, plus the Germany-wide average as region 'de'.
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

UNION ALL

SELECT
    'de' AS region_code,
    ts_utc,
    shortwave_radiation,
    cloud_cover,
    wind_speed_100m,
    temperature_2m,
    n_locations,
    CAST(NULL AS TIMESTAMP) AS _source_loaded_at
FROM {{ source('silver', 'weather_national_hourly') }}
