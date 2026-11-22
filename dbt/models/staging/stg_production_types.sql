SELECT
    series_name,
    smard_filter_id,
    name_en,
    name_de,
    category,
    is_renewable,
    is_weather_dependent
FROM {{ source('silver', 'ref_production_type') }}
