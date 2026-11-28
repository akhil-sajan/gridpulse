-- Grain: one row per German local day. Features for the daily solar forecast (P6.3):
-- national capacity-weighted weather, season, installed solar capacity, and the target
-- (solar generation that day). Only complete days: all quarter-hours of solar data and
-- at least 23 hours of weather (DST days have 23 or 25 hours), so the table runs from
-- 2020-01-01 to the newest day with final weather (Open-Meteo's archive is a few days late).
WITH hours AS (
    SELECT
        t.local_date,
        w.shortwave_radiation_w_m2,
        w.cloud_cover_pct,
        w.temperature_2m_c
    FROM {{ ref('fact_weather') }} AS w
    INNER JOIN {{ ref('dim_region') }} AS r USING (region_key)
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    WHERE r.region_code = 'de'
),

weather_daily AS (
    SELECT
        local_date,
        count(*) AS weather_hours,
        -- hourly means x 1 h = energy per square metre over the day
        round(sum(shortwave_radiation_w_m2), 1) AS radiation_wh_m2,
        round(max(shortwave_radiation_w_m2), 1) AS radiation_peak_w_m2,
        round(avg(cloud_cover_pct), 1) AS cloud_cover_pct,
        -- cloud cover while the sun is up matters more than at night
        round(avg(CASE WHEN shortwave_radiation_w_m2 > 0 THEN cloud_cover_pct END), 1)
            AS cloud_cover_daylight_pct,
        count_if(shortwave_radiation_w_m2 > 0) AS daylight_hours,
        round(avg(temperature_2m_c), 1) AS temperature_c
    FROM hours
    GROUP BY local_date
),

solar_daily AS (
    SELECT
        t.local_date,
        sum(g.mwh) AS solar_mwh,
        count(*) AS solar_quarter_hours
    FROM {{ ref('fact_generation') }} AS g
    INNER JOIN {{ ref('dim_time') }} AS t USING (time_key)
    INNER JOIN {{ ref('dim_production_type') }} AS p USING (production_type_key)
    WHERE p.series_name = 'solar'
    GROUP BY t.local_date
),

expected AS (
    SELECT
        local_date,
        count(*) AS quarter_hours
    FROM {{ ref('dim_time') }}
    GROUP BY local_date
),

-- Capacity known before the day starts: the end of the previous month (for January 2020,
-- the first month in the register data, its own end of month).
capacity AS (
    SELECT
        c.month_start,
        sum(c.installed_mw) AS solar_capacity_mw
    FROM {{ ref('fact_installed_capacity') }} AS c
    INNER JOIN {{ ref('dim_production_type') }} AS p USING (production_type_key)
    WHERE p.series_name = 'solar'
    GROUP BY c.month_start
)

SELECT
    w.local_date,
    dayofyear(w.local_date) AS day_of_year,
    -- season as a circle, so 31 December and 1 January are neighbours
    round(sin(2 * pi() * dayofyear(w.local_date) / 365.25), 4) AS day_of_year_sin,
    round(cos(2 * pi() * dayofyear(w.local_date) / 365.25), 4) AS day_of_year_cos,
    w.radiation_wh_m2,
    w.radiation_peak_w_m2,
    w.cloud_cover_pct,
    w.cloud_cover_daylight_pct,
    w.daylight_hours,
    w.temperature_c,
    round(coalesce(prev.solar_capacity_mw, cur.solar_capacity_mw), 1) AS solar_capacity_mw,
    round(s.solar_mwh, 1) AS solar_mwh,
    -- target normalised by capacity: share of the maximum possible output that day
    round(
        s.solar_mwh / (coalesce(prev.solar_capacity_mw, cur.solar_capacity_mw) * 24), 4
    ) AS solar_capacity_factor
FROM weather_daily AS w
INNER JOIN solar_daily AS s USING (local_date)
INNER JOIN expected AS e USING (local_date)
LEFT JOIN capacity AS prev
    ON prev.month_start = add_months(date_trunc('MONTH', w.local_date), -1)
LEFT JOIN capacity AS cur
    ON cur.month_start = date_trunc('MONTH', w.local_date)
WHERE
    w.weather_hours >= 23
    AND s.solar_quarter_hours = e.quarter_hours
    AND w.local_date >= DATE '2020-01-01'
    AND w.local_date < current_date()
