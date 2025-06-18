{{ config(
  materialized = 'table',
  tags = ['swep', 'swep_production_summary', 'sl']
) }}

WITH source_data AS (
    SELECT
        id,
        data
    FROM {{ source('staging_sl', 'SWEP_Production_Details') }}
),

base AS (
    SELECT
        id AS form_id,
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        NULLIF(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_production_sp', '') AS date_of_production_sp,
        jsonb_path_query(data::jsonb, '$.form.product_made_sp[*]') AS product_item
    FROM source_data
),

exploded AS (
    SELECT
        form_id,
        case_id,
        date_of_production_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'product_made_sp' AS product_name,
        (product_item::jsonb -> 'details_of_products_made_sp' ->> 'revenue_sp')::numeric AS revenue,
        COALESCE((product_item::jsonb -> 'details_of_products_made_sp' ->> 'attendees_sp')::int, 0) AS attendees
    FROM base
),

filtered AS (
    SELECT *
    FROM exploded
    WHERE product_name ILIKE 'exhibition'
)

SELECT
    COUNT(*) AS total_exhibitions,
    SUM(revenue) AS total_revenue_kes,
    ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2) AS average_sales_per_exhibition,
    ROUND(AVG(attendees), 1) AS average_attendees_per_exhibition
FROM filtered
