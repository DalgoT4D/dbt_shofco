{{ config(
    materialized='view',
    schema='sl',
    alias='sl_business_grants_quarterly_summary',
    tags=['sl', 'grants', 'quarterly_summary']
) }}

WITH source AS (
    SELECT 
        case_id,
        grant_amount,
        amount_requested,
        EXTRACT(YEAR FROM grant_allocated_date::date) AS year_allocated,
        EXTRACT(QUARTER FROM grant_allocated_date::date) AS quarter_allocated
    FROM {{ ref('sl_business_grants_analytics') }}
)

SELECT 
    year_allocated,
    quarter_allocated,
    COUNT(DISTINCT case_id) AS total_grants_count,
    SUM(grant_amount) AS total_grant_amount,
    SUM(amount_requested) AS total_amount_requested
FROM source
GROUP BY 
    year_allocated,
    quarter_allocated
ORDER BY 
    year_allocated DESC,
    quarter_allocated DESC
