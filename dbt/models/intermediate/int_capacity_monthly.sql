-- Installed capacity at the end of each month since 2020, per state and production type.
--
-- A unit (or a cohort of small units) counts at the end of month M when it was
-- commissioned in or before M and not decommissioned in or before M. Planned units and
-- units marked closed without a closing date are left out (they never ran, or we don't
-- know when they stopped). Temporarily stopped units are still installed capacity.
-- Month granularity: a unit commissioned on any day of M counts from the end of M.
WITH months AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2020-01-01' as date)",
        end_date="add_months(date_trunc('MM', current_date()), 1)"
    ) }}
),

cohorts AS (
    SELECT
        region_code,
        series_name,
        CAST(date_trunc('MM', commissioning_date) AS DATE) AS commissioning_month,
        CAST(date_trunc('MM', decommissioning_date) AS DATE) AS decommissioning_month,
        1 AS units,
        net_capacity_kw,
        CAST(0 AS DECIMAL(16, 3)) AS small_units_kw,
        CASE WHEN change_source = 'simulator' THEN net_capacity_kw ELSE 0 END
            AS simulated_kw
    FROM {{ ref('stg_power_plants') }}
    WHERE status <> 'In Planung'
      AND NOT (status = 'Endgültig stillgelegt' AND decommissioning_date IS NULL)

    UNION ALL

    SELECT
        region_code,
        series_name,
        commissioning_month,
        decommissioning_month,
        units,
        net_capacity_kw,
        net_capacity_kw AS small_units_kw,
        0 AS simulated_kw
    FROM {{ ref('stg_small_units') }}
    WHERE status <> 'In Planung'
      AND NOT (status = 'Endgültig stillgelegt' AND decommissioning_month IS NULL)
)

SELECT
    CAST(m.date_month AS DATE) AS month_start,
    c.region_code,
    c.series_name,
    sum(c.units) AS units,
    round(sum(c.net_capacity_kw) / 1000, 3) AS installed_mw,
    round(sum(c.small_units_kw) / 1000, 3) AS small_units_mw,
    round(sum(c.simulated_kw) / 1000, 3) AS simulated_mw
FROM months AS m
INNER JOIN cohorts AS c
    ON c.commissioning_month <= CAST(m.date_month AS DATE)
   AND (c.decommissioning_month IS NULL OR c.decommissioning_month > CAST(m.date_month AS DATE))
WHERE c.region_code IS NOT NULL
  AND c.series_name IS NOT NULL
GROUP BY 1, 2, 3
