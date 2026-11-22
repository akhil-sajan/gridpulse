-- P2.5 done-when: joining generation, price and weather for one day must not multiply rows.
-- Returns rows (= test fails) if the join produces more rows than generation alone.
WITH day_keys AS (
    SELECT time_key FROM {{ ref('dim_time') }} WHERE local_date = DATE'2024-06-15'
),

generation AS (
    SELECT g.* FROM {{ ref('fact_generation') }} AS g JOIN day_keys USING (time_key)
),

joined AS (
    SELECT g.time_key, g.production_type_key
    FROM generation AS g
    LEFT JOIN {{ ref('fact_price') }} AS p USING (time_key)
    LEFT JOIN {{ ref('fact_weather') }} AS w
        ON w.time_key = CAST(date_format(date_trunc('HOUR', to_timestamp(CAST(g.time_key AS STRING), 'yyyyMMddHHmm')), 'yyyyMMddHHmm') AS BIGINT)
       AND w.region_key = {{ dbt_utils.generate_surrogate_key(["'de'"]) }}
)

SELECT 'join multiplies rows' AS problem
FROM (SELECT count(*) AS n FROM joined) AS j
CROSS JOIN (SELECT count(*) AS n FROM generation) AS g
WHERE j.n <> g.n OR g.n = 0
