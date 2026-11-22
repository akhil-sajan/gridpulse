SELECT
    {{ dbt_utils.generate_surrogate_key(['series_name']) }} AS production_type_key,
    series_name,
    name_en,
    name_de,
    category,
    is_renewable,
    is_weather_dependent,
    smard_filter_id
FROM {{ ref('stg_production_types') }}
