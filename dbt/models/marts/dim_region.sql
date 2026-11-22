-- 16 federal states (one weather point each) plus Germany as a whole ('de').
SELECT
    {{ dbt_utils.generate_surrogate_key(['region_code']) }} AS region_key,
    region_code,
    region_name,
    weather_point,
    latitude,
    longitude,
    is_national
FROM {{ ref('regions') }}
