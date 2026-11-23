-- MaStR units below 100 kW, summed per cohort (same type, source, state, status and
-- commissioning/decommissioning month), with GridPulse region and production type.
SELECT
    s.unit_type,
    s.energy_source,
    s.federal_state,
    r.region_code,
    CASE
        WHEN m.series_name = 'wind_onshore' AND r.region_code = 'de_awz' THEN 'wind_offshore'
        ELSE nullif(m.series_name, '')
    END AS series_name,
    s.status,
    s.commissioning_month,
    s.decommissioning_month,
    s.units,
    s.net_capacity_kw,
    s.export_date
FROM {{ source('bronze', 'mastr_small_units_agg') }} AS s
LEFT JOIN {{ ref('regions') }} AS r
    ON r.region_name = s.federal_state AND NOT r.is_national
LEFT JOIN {{ ref('mastr_production_types') }} AS m
    ON m.unit_type = s.unit_type AND m.energy_source = s.energy_source
