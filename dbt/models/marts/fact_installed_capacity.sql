-- Grain: one row per month (capacity at month end), region (16 states + offshore zone)
-- and production type. Installed capacity in MW from the MaStR register: units >= 100 kW
-- one by one, smaller units as sums (small_units_mw shows how much of the total that is).
-- simulated_mw = capacity of units whose current state comes from the change simulator.
SELECT
    cast(date_format(c.month_start, 'yyyyMM') AS INT) AS month_key,
    c.month_start,
    last_day(c.month_start) AS month_end,
    {{ dbt_utils.generate_surrogate_key(['c.region_code']) }} AS region_key,
    {{ dbt_utils.generate_surrogate_key(['c.series_name']) }} AS production_type_key,
    c.units,
    c.installed_mw,
    c.small_units_mw,
    c.simulated_mw
FROM {{ ref('int_capacity_monthly') }} AS c
