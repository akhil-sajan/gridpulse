-- P6.3 done-when: features_solar_daily has a row for every day from 2020-01-01 to its
-- newest day (no missing days). Returns a row (= test fails) if days are missing.
SELECT
    min(local_date) AS first_day,
    max(local_date) AS last_day,
    count(*) AS days,
    datediff(max(local_date), min(local_date)) + 1 AS expected_days
FROM {{ ref('features_solar_daily') }}
HAVING
    min(local_date) <> DATE '2020-01-01'
    OR count(*) <> datediff(max(local_date), min(local_date)) + 1
    OR max(local_date) < date_sub(current_date(), 14)
