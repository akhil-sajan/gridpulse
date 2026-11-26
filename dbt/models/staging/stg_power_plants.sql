-- Current version of each MaStR unit >= 100 kW, with GridPulse region and production type.
-- Business time (commissioning/decommissioning dates) drives capacity history; the
-- SCD2 validity columns only say when the register record changed.
SELECT
    p.unit_id,
    p.unit_type,
    p.energy_source,
    p.federal_state,
    r.region_code,
    CASE
        WHEN m.series_name = 'wind_onshore' AND r.region_code = 'de_awz' THEN 'wind_offshore'
        ELSE nullif(m.series_name, '')
    END AS series_name,
    p.status,
    p.net_capacity_kw,
    p.commissioning_date,
    p.decommissioning_date,
    p.change_source,
    p.updated_at
FROM {{ source('lakeflow_gold', 'dim_power_plant') }} AS p
LEFT JOIN {{ ref('regions') }} AS r
    ON r.region_name = p.federal_state AND NOT r.is_national
LEFT JOIN {{ ref('mastr_production_types') }} AS m
    ON m.unit_type = p.unit_type AND m.energy_source = p.energy_source
WHERE p.__end_at IS NULL
