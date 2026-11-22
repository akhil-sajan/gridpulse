{{ config(liquid_clustered_by=['time_key']) }}
-- One row per 15-minute interval (UTC start) from December 2019 to the end of next year.
-- time_key = UTC start as yyyyMMddHHmm, so it is readable and unique even in the repeated
-- local hour on the October DST day.
WITH spine AS (
    SELECT explode(sequence(
        TIMESTAMP'2019-12-01 00:00:00',
        CAST(add_months(date_trunc('YEAR', current_date()), 24) AS TIMESTAMP) - INTERVAL 15 MINUTES,
        INTERVAL 15 MINUTES
    )) AS ts_utc
),

enriched AS (
    SELECT
        ts_utc,
        from_utc_timestamp(ts_utc, 'Europe/Berlin') AS ts_local
    FROM spine
)

SELECT
    CAST(date_format(e.ts_utc, 'yyyyMMddHHmm') AS BIGINT) AS time_key,
    e.ts_utc,
    e.ts_local,
    to_date(e.ts_local) AS local_date,
    year(e.ts_local) AS local_year,
    quarter(e.ts_local) AS local_quarter,
    month(e.ts_local) AS local_month,
    date_format(e.ts_local, 'MMMM') AS local_month_name,
    day(e.ts_local) AS local_day,
    hour(e.ts_local) AS local_hour,
    minute(e.ts_local) AS local_minute,
    hour(e.ts_local) * 4 + minute(e.ts_local) DIV 15 AS local_quarter_hour_of_day,
    weekday(e.ts_local) + 1 AS local_weekday,  -- 1 = Monday ... 7 = Sunday
    date_format(e.ts_local, 'EEEE') AS local_weekday_name,
    weekday(e.ts_local) >= 5 AS is_weekend,
    h.holiday_date IS NOT NULL AS is_public_holiday_de,
    h.name_en AS public_holiday_name,
    (unix_timestamp(e.ts_local) - unix_timestamp(e.ts_utc)) = 7200 AS is_summer_time,
    minute(e.ts_utc) = 0 AS is_full_hour
FROM enriched AS e
LEFT JOIN {{ ref('holidays_de') }} AS h
    ON to_date(e.ts_local) = h.holiday_date
