{{ config(materialized='view') }}
-- Grain: one row per usage day, product and workload. Compute in DBUs from the billing
-- system table, with names from the Lakeflow system tables and the list price in USD.
-- Free Edition doesn't bill; list_price_usd shows what the same usage would cost on a
-- paid plan (P5.7).
WITH usage AS (
    SELECT
        u.usage_date,
        u.billing_origin_product AS product,
        u.sku_name,
        u.usage_end_time,
        u.usage_quantity AS dbu,
        -- struct fields; sqlfluff reads a.b.c as table.column
        u.usage_metadata.job_id AS job_id,  -- noqa: RF01, AL09
        u.usage_metadata.dlt_pipeline_id AS pipeline_id,  -- noqa: RF01
        u.usage_metadata.warehouse_id AS warehouse_id  -- noqa: RF01, AL09
    FROM {{ source('system_billing', 'usage') }} AS u
    WHERE
        u.usage_unit = 'DBU'
        AND u.usage_date >= DATE '2026-11-01'  -- GridPulse started in November 2026
),

-- The Lakeflow tables keep one row per change; the newest row has the current name.
jobs AS (
    SELECT
        j.job_id,
        j.name
    FROM {{ source('system_lakeflow', 'jobs') }} AS j
    QUALIFY row_number() OVER (PARTITION BY j.job_id ORDER BY j.change_time DESC) = 1
),

pipelines AS (
    SELECT
        p.pipeline_id,
        p.name
    FROM {{ source('system_lakeflow', 'pipelines') }} AS p
    QUALIFY row_number() OVER (PARTITION BY p.pipeline_id ORDER BY p.change_time DESC) = 1
),

prices AS (
    SELECT
        lp.sku_name,
        lp.price_start_time,
        lp.price_end_time,
        cast(lp.pricing.default AS DOUBLE) AS usd_per_dbu  -- noqa: RF01
    FROM {{ source('system_billing', 'list_prices') }} AS lp
    WHERE lp.currency_code = 'USD' AND lp.usage_unit = 'DBU'
),

named AS (
    SELECT
        u.usage_date,
        u.product,
        u.dbu,
        pr.usd_per_dbu,
        coalesce(
            j.name,
            pl.name,
            CASE WHEN u.warehouse_id IS NOT NULL THEN 'sql_warehouse' END,
            lower(u.product)
        ) AS workload
    FROM usage AS u
    LEFT JOIN jobs AS j ON u.job_id = j.job_id
    LEFT JOIN pipelines AS pl ON u.pipeline_id = pl.pipeline_id
    LEFT JOIN prices AS pr
        ON
            u.sku_name = pr.sku_name
            AND u.usage_end_time >= pr.price_start_time
            AND (pr.price_end_time IS NULL OR u.usage_end_time < pr.price_end_time)
)

SELECT
    usage_date,
    product,
    workload,
    CASE
        WHEN workload LIKE '[dev %' THEN 'dev'
        WHEN workload LIKE 'gridpulse%' THEN 'prod'
        ELSE 'shared'
    END AS environment,
    -- Shared = SQL warehouse (dbt, CI, ad-hoc queries), notebooks, table optimization.
    workload LIKE '%gridpulse%' AS is_gridpulse_workload,
    round(sum(dbu), 4) AS dbu,
    round(sum(dbu * usd_per_dbu), 4) AS list_price_usd
FROM named
GROUP BY 1, 2, 3, 4, 5
