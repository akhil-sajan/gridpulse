-- Germany-wide hourly weather, weighted by where the plants are (P3.5).
--
-- A plain average of the 16 state capitals treats Berlin like Lower Saxony. Here each
-- state's value is weighted by the capacity that reacts to it, at that month:
-- radiation and cloud cover by solar capacity, wind speed by onshore wind capacity.
-- Temperature stays a plain average. The offshore zone has no weather point.
WITH weights AS (
    SELECT
        month_start,
        region_code,
        sum(CASE WHEN series_name = 'solar' THEN installed_mw ELSE 0 END) AS solar_mw,
        sum(CASE WHEN series_name = 'wind_onshore' THEN installed_mw ELSE 0 END) AS wind_mw
    FROM {{ ref('int_capacity_monthly') }}
    GROUP BY 1, 2
),

weather AS (
    SELECT
        w.*,
        coalesce(k.solar_mw, 0) AS solar_mw,
        coalesce(k.wind_mw, 0) AS wind_mw
    FROM {{ ref('stg_openmeteo_weather') }} AS w
    LEFT JOIN weights AS k
        ON k.region_code = w.region_code
       AND k.month_start = CAST(date_trunc('MM', w.ts_utc) AS DATE)
)

-- Rounded: a weighted mean of values that are all 100 can come out as 100.00000000000001
-- in floating point, which would break the 0-100 range check on cloud cover.
SELECT
    'de' AS region_code,
    ts_utc,
    round(
        sum(shortwave_radiation * solar_mw)
        / nullif(sum(CASE WHEN shortwave_radiation IS NOT NULL THEN solar_mw END), 0),
        2
    ) AS shortwave_radiation,
    round(
        sum(cloud_cover * solar_mw)
        / nullif(sum(CASE WHEN cloud_cover IS NOT NULL THEN solar_mw END), 0),
        2
    ) AS cloud_cover,
    round(
        sum(wind_speed_100m * wind_mw)
        / nullif(sum(CASE WHEN wind_speed_100m IS NOT NULL THEN wind_mw END), 0),
        2
    ) AS wind_speed_100m,
    round(avg(temperature_2m), 2) AS temperature_2m,
    count(*) AS n_locations,
    max(_source_loaded_at) AS _source_loaded_at
FROM weather
GROUP BY ts_utc
